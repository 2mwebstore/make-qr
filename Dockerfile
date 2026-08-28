# ---- build stage ----
FROM golang:1.23-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o server .

# ---- runtime stage ----
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    ca-certificates \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/server ./server

# Tells the code (which checks os.Getenv("CHROME_PATH")) where to find
# the browser binary instead of trying to launch its own.
ENV CHROME_PATH=/usr/bin/chromium

# Railway sets PORT itself; the code already reads it via getenv("PORT", "8080").
EXPOSE 8080

CMD ["./server"]