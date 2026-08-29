package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

const (
	paywayURL = "https://link.payway.com.kh/ABAPAY7R5083671"
)

// domQRScript looks for an element whose value attribute holds an EMV
// QR payload (KHQR strings start with "000201") and returns it, or ""
// if none is present yet. Used to poll the DOM directly in case the
// checkout page renders the QR string into the page rather than (or
// in addition to) returning it in a JSON response.
const domQRScript = `
(() => {
	const els = document.querySelectorAll('[value]');
	for (const el of els) {
		const v = el.getAttribute('value') || '';
		if (v.length > 60 && v.indexOf('000201') === 0) {
			return v;
		}
	}
	return '';
})()
`

type Payment struct {
	ID          int64  `json:"id"`
	Amount      string `json:"amount"`
	QRString    string `json:"qr_string,omitempty"`
	DeviceID    string `json:"device_id,omitempty"`
	RequestTime string `json:"request_time,omitempty"`
	ClientID    string `json:"client_id,omitempty"`
	Hash        string `json:"hash,omitempty"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`

	mu     sync.RWMutex
	cancel context.CancelFunc
}

// QRResult is everything captured off the ABA checkout page that the
// caller needs to hit ABA's own check-transaction ("hook") API directly,
// without keeping a browser tab open.
type QRResult struct {
	QRString    string
	DeviceID    string
	RequestTime string
	ClientID    string
	Hash        string
}

// ready reports whether every field we expect from the ABA checkout
// flow has been captured.
func (r QRResult) ready() bool {
	return r.QRString != "" &&
		r.DeviceID != "" &&
		r.ClientID != "" &&
		r.Hash != ""
}

type App struct {
	mu       sync.RWMutex
	payments map[int64]*Payment
	nextID   atomic.Int64

	// browserCtx is a single, long-lived Chrome instance shared across
	// every request. Each payment opens a new tab inside it instead of
	// launching a fresh browser process, which is what was costing
	// several seconds per request.
	browserCtx context.Context

	// browserReady is closed once Chrome startup finishes — success or
	// failure. Everything that needs the browser waits on this instead
	// of blocking the whole process (and the HTTP listener) at boot.
	// Written once, before the close, from initBrowser's goroutine;
	// the close establishes happens-before for any reader.
	browserReady chan struct{}
	browserErr   error

	// warmTabs holds tabs that are already navigated to the PayWay
	// checkout page and idle, ready to be handed to a request. This
	// takes page-load latency out of the request's critical path.
	warmTabs chan warmTab
	poolSize int

	// tabSetupSem limits how many tab-setup operations (navigate + wait
	// for the amount form) run at once, across both the warm-pool
	// background goroutine and any request that falls through to the
	// fresh-tab path. Prevents a burst of concurrent requests from all
	// hammering the one shared Chrome process simultaneously.
	tabSetupSem chan struct{}
}

// warmTab is a pre-navigated tab sitting idle in the pool.
type warmTab struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type CreatePaymentRequest struct {
	Amount interface{} `json:"amount"`
}

func main() {
	// Pool/concurrency sizes are deliberately conservative — headless
	// Chrome is memory-hungry, and on a small container (e.g. a basic
	// Railway plan) each extra idle tab or concurrent setup raises the
	// odds of an OOM kill, which is what silently kills the shared
	// browser context and used to make every request fail afterward
	// with "context canceled" until now. If you're on a larger plan
	// with memory to spare, these can be raised for more throughput.
	app := &App{
		payments:     make(map[int64]*Payment),
		warmTabs:     make(chan warmTab, 1),
		poolSize:     1,
		browserReady: make(chan struct{}),
		tabSetupSem:  make(chan struct{}, 2),
	}

	// Chrome starts in the background — the HTTP server (and /health)
	// comes up immediately regardless of how long Chrome takes, or
	// whether it fails entirely.
	go app.initBrowser()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", app.health)
	mux.HandleFunc("POST /api/payments/qr", app.createPayment)
	mux.HandleFunc("GET /api/payments/{id}/status", app.paymentStatus)

	port := getenv("PORT", "8080")

	if err := http.ListenAndServe(
		":"+port,
		cors(mux),
	); err != nil {
		fmt.Fprintln(os.Stderr, "http server failed:", err)
		os.Exit(1)
	}
}

// initBrowser launches the shared Chrome instance and, on success,
// keeps the warm-tab pool topped up for the rest of the process's
// life. On failure it records the error (surfaced via /health and via
// any payment request, which will fail with this same message) rather
// than crashing the whole process — the HTTP server stays up either
// way.
// chromeCandidatePaths are checked in order when CHROME_PATH isn't
// set, or points at something that doesn't actually exist. Covers the
// binary names/paths the common Debian/Ubuntu chromium and
// google-chrome packages actually install.
var chromeCandidatePaths = []string{
	"/usr/bin/chromium",
	"/usr/bin/chromium-browser",
	"/usr/bin/google-chrome",
	"/usr/bin/google-chrome-stable",
	"/usr/lib/chromium/chromium",
}

// findChromeExecPath returns a real, existing browser binary path.
// CHROME_PATH is honored if set AND the file actually exists there;
// otherwise every candidate in chromeCandidatePaths is checked via
// os.Stat (a real filesystem check, not chromedp's own internal
// guesswork) and the first hit is used. If nothing is found, the
// error lists every path that was tried so the real misconfiguration
// is visible instead of a generic "no such file" for one guessed name.
func findChromeExecPath() (string, error) {
	tried := []string{}

	if envPath := os.Getenv("CHROME_PATH"); envPath != "" {
		tried = append(tried, envPath)
		if info, err := os.Stat(envPath); err == nil && !info.IsDir() {
			return envPath, nil
		}
	}

	for _, p := range chromeCandidatePaths {
		tried = append(tried, p)
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			return p, nil
		}
	}

	return "", fmt.Errorf(
		"no chrome/chromium binary found — tried: %s",
		strings.Join(tried, ", "),
	)
}

// launchBrowser starts a fresh Chrome process and returns its
// long-lived browser-level context. Used both for the initial startup
// and for self-healing relaunches if Chrome later crashes.
func (a *App) launchBrowser() (context.Context, error) {
	allocOpts := append(
		chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag(
			"disable-blink-features",
			"AutomationControlled",
		),
		chromedp.Flag("window-size", "1280,900"),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-background-networking", true),
		chromedp.Flag("disable-sync", true),
		chromedp.Flag("disable-default-apps", true),
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("metrics-recording-only", true),
		chromedp.Flag("mute-audio", true),
	)

	execPath, err := findChromeExecPath()
	if err != nil {
		return nil, err
	}
	allocOpts = append(allocOpts, chromedp.ExecPath(execPath))

	allocCtx, _ := chromedp.NewExecAllocator(
		context.Background(),
		allocOpts...,
	)

	browserCtx, _ := chromedp.NewContext(allocCtx)

	// Bound only the initial process launch — if Chrome ever hangs on
	// startup, this must not hang forever, since that would block
	// browserReady from ever closing and stall every request.
	launchCtx, launchCancel := context.WithTimeout(browserCtx, 30*time.Second)
	defer launchCancel()

	if err := chromedp.Run(launchCtx); err != nil {
		return nil, fmt.Errorf("chrome launch failed: %w", err)
	}

	return browserCtx, nil
}

func (a *App) initBrowser() {
	browserCtx, err := a.launchBrowser()
	if err != nil {
		a.browserErr = err
		fmt.Fprintln(os.Stderr, err)
		close(a.browserReady)
		return
	}

	a.mu.Lock()
	a.browserCtx = browserCtx
	a.mu.Unlock()

	close(a.browserReady)

	a.maintainWarmPool()
}

// getBrowserCtx returns a live browser context, transparently
// relaunching Chrome if the previous instance has died — e.g. crashed
// under memory pressure — instead of every request failing forever
// with "context canceled" once that happens once.
func (a *App) getBrowserCtx() (context.Context, error) {
	<-a.browserReady

	a.mu.RLock()
	ctx := a.browserCtx
	berr := a.browserErr
	a.mu.RUnlock()

	if berr != nil {
		return nil, berr
	}

	if ctx != nil && ctx.Err() == nil {
		return ctx, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Re-check under the lock — another request may have already
	// relaunched while we were waiting for it.
	if a.browserCtx != nil && a.browserCtx.Err() == nil {
		return a.browserCtx, nil
	}

	fmt.Fprintln(os.Stderr, "chrome context dead — relaunching")

	newCtx, err := a.launchBrowser()
	if err != nil {
		return nil, fmt.Errorf("chrome relaunch failed: %w", err)
	}

	a.browserCtx = newCtx
	return newCtx, nil
}

func (a *App) health(
	w http.ResponseWriter,
	r *http.Request,
) {
	resp := map[string]interface{}{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	}

	select {
	case <-a.browserReady:
		a.mu.RLock()
		ctx := a.browserCtx
		berr := a.browserErr
		a.mu.RUnlock()

		switch {
		case berr != nil:
			resp["browser"] = "error"
			resp["browser_error"] = berr.Error()
		case ctx != nil && ctx.Err() != nil:
			// Launched fine initially but has since died (e.g. OOM
			// kill) — the old handler could never report this since
			// it only ever checked the initial-launch error.
			resp["browser"] = "crashed"
			resp["browser_error"] = ctx.Err().Error()
		default:
			resp["browser"] = "ready"
		}
	default:
		resp["browser"] = "starting"
	}

	writeJSON(
		w,
		http.StatusOK,
		resp,
	)
}

func (a *App) createPayment(
	w http.ResponseWriter,
	r *http.Request,
) {
	start := time.Now()

	var input CreatePaymentRequest

	if err := json.NewDecoder(
		r.Body,
	).Decode(&input); err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"invalid JSON body",
		)
		return
	}

	amount, err := normalizeAmount(
		input.Amount,
	)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			err.Error(),
		)
		return
	}

	id := a.nextID.Add(1)

	payment := &Payment{
		ID:        id,
		Amount:    amount,
		Status:    "creating_qr",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	a.mu.Lock()
	a.payments[id] = payment
	a.mu.Unlock()

	result, err := a.startPayWayBrowser(
		payment,
	)
	if err != nil {
		payment.mu.Lock()
		payment.Status = "error"
		payment.mu.Unlock()

		writeError(
			w,
			http.StatusBadGateway,
			err.Error(),
		)
		return
	}

	payment.mu.Lock()
	payment.QRString = result.QRString
	payment.DeviceID = result.DeviceID
	payment.RequestTime = result.RequestTime
	payment.ClientID = result.ClientID
	payment.Hash = result.Hash
	if payment.Status != "approved" {
		payment.Status = "request_qr"
	}
	payment.mu.Unlock()

	// Step 1 response: qr_string plus the device/client/hash fields
	// needed to hit ABA's check-transaction API directly. The browser
	// tab is already closed by this point (see startPayWayBrowser) —
	// no browser session is kept alive for approval polling anymore.
	writeJSON(
		w,
		http.StatusCreated,
		map[string]interface{}{
			"qr_string":    payment.QRString,
			"device_id":    payment.DeviceID,
			"request_time": payment.RequestTime,
			"client_id":    payment.ClientID,
			"hash":         payment.Hash,
			"elapsed_ms":   time.Since(start).Milliseconds(),
		},
	)
}

func (a *App) paymentStatus(
	w http.ResponseWriter,
	r *http.Request,
) {
	id, err := strconv.ParseInt(
		r.PathValue("id"),
		10,
		64,
	)

	if err != nil || id <= 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"invalid payment id",
		)
		return
	}

	a.mu.RLock()
	payment, ok := a.payments[id]
	a.mu.RUnlock()

	if !ok {
		writeError(
			w,
			http.StatusNotFound,
			"payment not found",
		)
		return
	}

	payment.mu.RLock()
	defer payment.mu.RUnlock()

	response := map[string]interface{}{
		"id":       payment.ID,
		"amount":   payment.Amount,
		"status":   payment.Status,
		"approved": payment.Status == "approved",
	}

	if payment.QRString != "" {
		response["qr_string"] = payment.QRString
	}

	if payment.DeviceID != "" {
		response["device_id"] = payment.DeviceID
	}

	if payment.RequestTime != "" {
		response["request_time"] = payment.RequestTime
	}

	if payment.ClientID != "" {
		response["client_id"] = payment.ClientID
	}

	if payment.Hash != "" {
		response["hash"] = payment.Hash
	}

	if payment.Status == "approved" {
		response["redirect_url"] = getenv(
			"APPROVED_REDIRECT_URL",
			"https://example.com/payment/success",
		)
	}

	writeJSON(
		w,
		http.StatusOK,
		response,
	)
}

// resourceBlockPatterns are URL patterns for asset types the checkout
// page doesn't need us to load — images, fonts, media. Blocking them
// cuts real time off page-ready since Chrome never fetches or decodes
// them. CSS/JS are left alone since the page's own logic and layout
// depend on them. These use the URLPattern spec CDP now expects
// (https://urlpattern.spec.whatwg.org/) — absolute patterns only,
// e.g. "*://*:*/*.png", not a bare "*.png" glob.
var resourceBlockPatterns = []*network.BlockPattern{
	{URLPattern: "*://*:*/*.png", Block: true},
	{URLPattern: "*://*:*/*.jpg", Block: true},
	{URLPattern: "*://*:*/*.jpeg", Block: true},
	{URLPattern: "*://*:*/*.gif", Block: true},
	{URLPattern: "*://*:*/*.svg", Block: true},
	{URLPattern: "*://*:*/*.webp", Block: true},
	{URLPattern: "*://*:*/*.ico", Block: true},
	{URLPattern: "*://*:*/*.woff", Block: true},
	{URLPattern: "*://*:*/*.woff2", Block: true},
	{URLPattern: "*://*:*/*.ttf", Block: true},
	{URLPattern: "*://*:*/*.otf", Block: true},
	{URLPattern: "*://*:*/*.eot", Block: true},
	{URLPattern: "*://*:*/*.mp4", Block: true},
	{URLPattern: "*://*:*/*.webm", Block: true},
	{URLPattern: "*://*:*/*.mp3", Block: true},
	{URLPattern: "*://*:*/*.wav", Block: true},
}

// newPreparedTab opens a fresh tab under the shared browser, enables
// the network domain, blocks non-essential assets, navigates to the
// PayWay checkout page, and waits for the amount form to be visible.
// The caller owns the returned cancel func. No per-payment timeout or
// event listener is attached here — those are added by whoever
// consumes the tab, since they need the specific payment in scope.
func (a *App) newPreparedTab() (context.Context, context.CancelFunc, error) {
	// Waits for the initial launch and transparently relaunches Chrome
	// if it has since crashed, instead of every call failing forever
	// once that happens.
	browserCtx, err := a.getBrowserCtx()
	if err != nil {
		return nil, nil, err
	}

	// Caps how many tab-setup operations run at once. Without this, a
	// burst of concurrent requests each navigates simultaneously,
	// which can overload a single shared Chrome process on a small
	// container — this is what was causing some requests to just
	// never respond under concurrent load.
	select {
	case a.tabSetupSem <- struct{}{}:
		defer func() { <-a.tabSetupSem }()
	case <-time.After(25 * time.Second):
		return nil, nil, fmt.Errorf("timed out waiting for a free browser slot")
	}

	ctx, cancel := chromedp.NewContext(browserCtx)

	// Bound the setup itself — if a navigation or selector wait ever
	// stalls (Chrome overloaded, page unresponsive), this must fail
	// instead of hanging the request forever. setupCancel only cancels
	// this child context, not the tab (ctx) itself, so the tab stays
	// usable afterward if setup succeeded in time.
	setupCtx, setupCancel := context.WithTimeout(ctx, 15*time.Second)
	defer setupCancel()

	if err := chromedp.Run(
		setupCtx,
		network.Enable().
			WithMaxPostDataSize(10*1024*1024).
			WithMaxTotalBufferSize(20*1024*1024).
			WithMaxResourceBufferSize(10*1024*1024),
		network.SetBlockedURLs().WithURLPatterns(resourceBlockPatterns),
		chromedp.Navigate(paywayURL),
		chromedp.WaitVisible(`input`, chromedp.ByQuery),
	); err != nil {
		cancel()
		return nil, nil, err
	}

	return ctx, cancel, nil
}

// maintainWarmPool keeps a.warmTabs topped up with tabs that are
// already sitting on the PayWay checkout page, so a payment request
// can skip navigation entirely and go straight to filling the form.
func (a *App) maintainWarmPool() {
	for {
		ctx, cancel, err := a.newPreparedTab()
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		// Blocks here once the pool is full — resumes as soon as a
		// request consumes one, which is exactly the replenishment
		// behavior we want.
		a.warmTabs <- warmTab{ctx: ctx, cancel: cancel}
	}
}

// takeTab returns a pre-navigated tab from the pool if one is ready
// within a short wait, otherwise prepares one fresh on the spot (the
// old, slower path — but still on the shared browser process).
// takeTab returns a pre-navigated tab from the pool if one is ready
// within a short wait, otherwise prepares one fresh on the spot (the
// old, slower path — but still on the shared browser process). The
// fresh-tab path retries once on failure: right after a cold start,
// the first attempt can occasionally get interrupted (e.g. Chrome's
// process still stabilizing) and return a transient error like
// "context canceled" — a single retry clears that without surfacing
// it to the caller.
func (a *App) takeTab() (context.Context, context.CancelFunc, error) {
	select {
	case t := <-a.warmTabs:
		return t.ctx, t.cancel, nil
	case <-time.After(300 * time.Millisecond):
	}

	ctx, cancel, err := a.newPreparedTab()
	if err != nil {
		ctx, cancel, err = a.newPreparedTab()
	}

	return ctx, cancel, err
}

func (a *App) startPayWayBrowser(
	payment *Payment,
) (QRResult, error) {
	tabCtx, cancelTab, err := a.takeTab()
	if err != nil {
		return QRResult{}, fmt.Errorf("prepare tab: %w", err)
	}

	// Absolute ceiling for the whole request, from amount entry through
	// waiting on ABA's own check-payment-status call. The actual flow
	// normally finishes in a few seconds; this is a safety net so a
	// stuck request fails within a bounded, sane window instead of
	// hanging for minutes (the 45s wait further below for the QR/hook
	// fields already covers the expected-slow case).
	ctx, timeoutCancel :=
		context.WithTimeout(
			tabCtx,
			90*time.Second,
		)

	cancelAll := func() {
		timeoutCancel()
		cancelTab()
	}

	payment.mu.Lock()
	payment.cancel = cancelAll
	payment.mu.Unlock()

	resultCh := make(chan QRResult, 1)
	errCh := make(chan error, 1)

	type trackedRequest struct {
		URL string
	}

	var trackedMu sync.Mutex
	tracked := make(
		map[network.RequestID]trackedRequest,
	)

	// collected accumulates fields as they show up across however many
	// POST requests/responses the checkout page fires. Guarded by
	// payment.mu since it's read/written from several goroutines.
	var collected QRResult
	var sentOnce sync.Once

	// finalize sends whatever has been collected so far and closes the
	// tab. Callers only invoke this once collected.ready() is true
	// (qr_string + device_id + client_id + hash all captured).
	finalize := func() {
		payment.mu.RLock()
		snapshot := collected
		payment.mu.RUnlock()

		sentOnce.Do(func() {
			select {
			case resultCh <- snapshot:
			default:
			}
			cancelAll()
		})
	}

	chromedp.ListenTarget(
		ctx,
		func(ev interface{}) {
			switch e := ev.(type) {

			case *network.EventRequestWillBeSent:
				if e.Request.Method != "POST" {
					return
				}

				if !strings.Contains(
					e.Request.URL,
					"pwapp.ababank.com",
				) {
					return
				}

				trackedMu.Lock()

				tracked[e.RequestID] =
					trackedRequest{
						URL: e.Request.URL,
					}

				trackedMu.Unlock()

				if len(
					e.Request.PostDataEntries,
				) > 0 {
					for _, entry := range e.Request.PostDataEntries {

						decoded, err :=
							base64.StdEncoding.
								DecodeString(
									entry.Bytes,
								)

						if err == nil {

							// ABA's checkout JS sends device_id/
							// request_time/client_id/hash as the
							// REQUEST body of check-payment-status,
							// not in any response — capture them here.
							var reqData interface{}

							if jsonErr := json.Unmarshal(
								decoded, &reqData,
							); jsonErr == nil {

								payment.mu.Lock()

								if v := findStringRecursive(
									reqData, "device_id",
								); v != "" {
									collected.DeviceID = v
									payment.DeviceID = v
								}

								if v := findStringRecursive(
									reqData, "request_time",
								); v != "" {
									collected.RequestTime = v
									payment.RequestTime = v
								}

								if v := findStringRecursive(
									reqData, "client_id",
								); v != "" {
									collected.ClientID = v
									payment.ClientID = v
								}

								if v := findStringRecursive(
									reqData, "hash",
								); v != "" {
									collected.Hash = v
									payment.Hash = v
								}

								ready := collected.ready()

								payment.mu.Unlock()

								// Fires once qr_string + device_id +
								// client_id + hash have all been seen —
								// this request usually supplies the
								// last three, right after qr_string was
								// already found via response/DOM.
								if ready {
									finalize()
								}
							}
						}
					}
				}

			case *network.EventLoadingFinished:

				trackedMu.Lock()

				_, ok := tracked[e.RequestID]

				if ok {
					delete(
						tracked,
						e.RequestID,
					)
				}

				trackedMu.Unlock()

				if !ok {
					return
				}

				requestID := e.RequestID

				go func() {

					time.Sleep(
						30 * time.Millisecond,
					)

					var body []byte

					err := chromedp.Run(
						ctx,
						chromedp.ActionFunc(
							func(ctx context.Context) error {

								var err error

								body, err =
									network.
										GetResponseBody(
											requestID,
										).
										Do(ctx)

								return err
							},
						),
					)

					if err != nil {

						return
					}

					// ==========================================
					// Parse JSON
					// ==========================================

					var data interface{}

					if err := json.Unmarshal(
						body,
						&data,
					); err != nil {
						return
					}

					// ==========================================
					// QR String + hook-API fields
					// ==========================================

					payment.mu.Lock()

					if v := findStringRecursive(
						data, "qr_string",
					); v != "" {
						collected.QRString = v
						payment.QRString = v
						if payment.Status != "approved" {
							payment.Status = "request_qr"
						}
					}

					if v := findStringRecursive(
						data, "device_id",
					); v != "" {
						collected.DeviceID = v
						payment.DeviceID = v
					}

					if v := findStringRecursive(
						data, "request_time",
					); v != "" {
						collected.RequestTime = v
						payment.RequestTime = v
					}

					if v := findStringRecursive(
						data, "client_id",
					); v != "" {
						collected.ClientID = v
						payment.ClientID = v
					}

					if v := findStringRecursive(
						data, "hash",
					); v != "" {
						collected.Hash = v
						payment.Hash = v
					}

					ready := collected.ready()

					payment.mu.Unlock()

					// Wait for the full set — qr_string, device_id,
					// client_id, hash — before responding and closing
					// the tab. device_id/hash typically only show up
					// once check-payment-status fires.
					if ready {
						finalize()
					}

					// ==========================================
					// Payment Action
					// ==========================================

					action :=
						findStringRecursive(
							data,
							"action",
						)

					if action != "" {

						payment.mu.Lock()

						payment.Status =
							action

						payment.mu.Unlock()
					}

				}()
			}
		},
	)

	var amountResult string

	if err := chromedp.Run(
		ctx,
		chromedp.Evaluate(
			`
			(() => {
				const amount = `+strconv.Quote(payment.Amount)+`;

				let input =
					document.querySelector('input[type="number"]') ||
					document.querySelector('input[inputmode="decimal"]') ||
					document.querySelector('input[inputmode="numeric"]') ||
					document.querySelector('input[placeholder*="amount" i]');

				if (!input) {
					const inputs =
						Array.from(
							document.querySelectorAll('input')
						);

					input =
						inputs.find(el => {
							const type =
								el.type || '';

							if (
								type === 'hidden' ||
								type === 'radio' ||
								type === 'checkbox'
							) {
								return false;
							}

							const rect =
								el.getBoundingClientRect();

							return (
								rect.width > 0 &&
								rect.height > 0
							);
						});
				}

				if (!input) {
					return "AMOUNT_INPUT_NOT_FOUND";
				}

				input.focus();

				const setter =
					Object.getOwnPropertyDescriptor(
						HTMLInputElement.prototype,
						"value"
					).set;

				setter.call(
					input,
					amount
				);

				input.dispatchEvent(
					new Event(
						"input",
						{
							bubbles: true
						}
					)
				);

				input.dispatchEvent(
					new Event(
						"change",
						{
							bubbles: true
						}
					)
				);

				input.blur();

				return (
					"AMOUNT_SET:" +
					input.value
				);
			})()
			`,
			&amountResult,
		),
		chromedp.Sleep(
			80*time.Millisecond,
		),
	); err != nil {
		cancelAll()

		return QRResult{},
			fmt.Errorf(
				"set amount: %w",
				err,
			)
	}

	if strings.Contains(
		amountResult,
		"NOT_FOUND",
	) {
		cancelAll()

		return QRResult{},
			fmt.Errorf(
				"amount input not found",
			)
	}

	var buttonResult string

	if err := chromedp.Run(
		ctx,
		chromedp.Evaluate(
			`
			(() => {
				const elements =
					Array.from(
						document.querySelectorAll(
							'button, a, div[role="button"], input[type="submit"]'
						)
					);

				const button =
					elements.find(el => {
						const text =
							(
								el.innerText ||
								el.value ||
								el.textContent ||
								''
							)
							.trim()
							.toLowerCase();

						const rect =
							el.getBoundingClientRect();

						return (
							rect.width > 0 &&
							rect.height > 0 &&
							text.includes('continue')
						);
					});

				if (!button) {
					return "CONTINUE_BUTTON_NOT_FOUND";
				}

				button.click();

				return "CLICKED";
			})()
			`,
			&buttonResult,
		),
	); err != nil {
		cancelAll()

		return QRResult{},
			fmt.Errorf(
				"click Continue: %w",
				err,
			)
	}

	if strings.Contains(
		buttonResult,
		"NOT_FOUND",
	) {
		cancelAll()

		return QRResult{},
			fmt.Errorf(
				"Continue button not found",
			)
	}

	// Some flows render the KHQR string directly into a DOM element's
	// value attribute (e.g. <div value="00020101...">) rather than —
	// or in addition to — sending it back in a JSON response we can
	// intercept. Poll for it in parallel with the network capture
	// above; whichever source finds it first wins. Polls at 100ms to
	// minimize the latency this adds on top of when ABA's own JS
	// actually writes the value into the DOM.
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		deadline := time.NewTimer(20 * time.Second)
		defer deadline.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-deadline.C:
				return

			case <-ticker.C:
				var found string

				if err := chromedp.Run(
					ctx,
					chromedp.Evaluate(
						domQRScript,
						&found,
					),
				); err != nil {
					continue
				}

				if found == "" {
					continue
				}

				payment.mu.Lock()

				if collected.QRString == "" {
					collected.QRString = found
					payment.QRString = found

					if payment.Status != "approved" {
						payment.Status = "request_qr"
					}
				}

				ready := collected.ready()

				payment.mu.Unlock()

				// qr_string is found — nothing more for this poller
				// to do. Don't finalize yet unless device_id/client_id/
				// hash are already in too; the network listener above
				// will finalize once check-payment-status supplies them.
				if ready {
					finalize()
				}

				return
			}
		}
	}()

	select {
	case result := <-resultCh:
		return result, nil

	case err := <-errCh:
		cancelAll()
		return QRResult{}, err

	case <-time.After(
		45 * time.Second,
	):
		cancelAll()

		return QRResult{},
			fmt.Errorf(
				"timeout waiting for QR response",
			)

	case <-ctx.Done():
		cancelAll()
		return QRResult{}, ctx.Err()
	}
}

func findStringRecursive(
	value interface{},
	wanted string,
) string {
	switch v := value.(type) {

	case map[string]interface{}:
		for key, item := range v {
			if key == wanted {
				switch x := item.(type) {
				case string:
					return x
				default:
					return fmt.Sprint(x)
				}
			}

			if found :=
				findStringRecursive(
					item,
					wanted,
				); found != "" {
				return found
			}
		}

	case []interface{}:
		for _, item := range v {
			if found :=
				findStringRecursive(
					item,
					wanted,
				); found != "" {
				return found
			}
		}
	}

	return ""
}

func normalizeAmount(
	value interface{},
) (string, error) {
	var raw string

	switch v := value.(type) {

	case string:
		raw = strings.TrimSpace(v)

	case float64:
		raw = strconv.FormatFloat(
			v,
			'f',
			-1,
			64,
		)

	default:
		return "",
			fmt.Errorf(
				"amount is required",
			)
	}

	number, err :=
		strconv.ParseFloat(
			raw,
			64,
		)

	if err != nil ||
		number <= 0 {

		return "",
			fmt.Errorf(
				"amount must be greater than zero",
			)
	}

	return strconv.FormatFloat(
		number,
		'f',
		-1,
		64,
	), nil
}

func requestBaseURL(
	r *http.Request,
) string {
	scheme :=
		r.Header.Get(
			"X-Forwarded-Proto",
		)

	if scheme == "" {
		scheme = "http"
	}

	host := r.Host

	if forwardedHost :=
		r.Header.Get(
			"X-Forwarded-Host",
		); forwardedHost != "" {
		host = forwardedHost
	}

	return scheme +
		"://" +
		host
}

func getenv(
	key string,
	fallback string,
) string {
	if value :=
		os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	value interface{},
) {
	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(status)

	_ = json.NewEncoder(
		w,
	).Encode(value)
}

func writeError(
	w http.ResponseWriter,
	status int,
	message string,
) {
	writeJSON(
		w,
		status,
		map[string]interface{}{
			"error": message,
		},
	)
}

func cors(
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			w.Header().Set(
				"Access-Control-Allow-Origin",
				"*",
			)

			w.Header().Set(
				"Access-Control-Allow-Headers",
				"Content-Type, Authorization",
			)

			w.Header().Set(
				"Access-Control-Allow-Methods",
				"GET, POST, OPTIONS",
			)

			if r.Method ==
				http.MethodOptions {

				w.WriteHeader(
					http.StatusNoContent,
				)

				return
			}

			next.ServeHTTP(
				w,
				r,
			)
		},
	)
}
