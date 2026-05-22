# ── Build stage ───────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build gateway binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o syncflow .

# Build worker binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o syncflow-worker ./cmd/worker

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/syncflow          /syncflow
COPY --from=builder /app/syncflow-worker   /syncflow-worker

EXPOSE 8080
ENTRYPOINT ["/syncflow"]
