# SyncFlow — Distributed Microservices Platform

Production-ready Go monorepo: fault-tolerant microservices, RabbitMQ worker pools, PostgreSQL persistence, OpenTelemetry distributed tracing, Grafana dashboards, and GitHub Actions CI/CD.

## Architecture

```
HTTP Clients
      │
      ▼
┌─────────────┐
│  Gateway    │  main.go — HTTP router, auth middleware
│  :8080      │
└──────┬──────┘
       │ publishes Task JSON
       ▼
┌─────────────────────┐
│  RabbitMQ           │  syncflow.exchange (topic)
│  Queue: syncflow.tasks │  DLX: syncflow.dlx
└──────────┬──────────┘
           │ N consumers
    ┌──────▼──────┐
    │  Worker Pool│  worker/worker.go
    │  8 goroutines│  per replica (×2 replicas)
    └──────┬──────┘
           │ INSERT results
           ▼
    ┌──────────────┐
    │  PostgreSQL  │  db/db.go — pgxpool (20 conns)
    │  task_results│
    └──────────────┘

Observability:
  All services → OTLP HTTP → Jaeger (traces)
                           → Grafana (metrics/dashboards)
```

## Files

| File / Dir | Purpose |
|------------|---------|
| `main.go` | API gateway — HTTP server, graceful shutdown, DI wiring |
| `worker/worker.go` | RabbitMQ consumer pool — N goroutine workers, retry + DLX |
| `db/db.go` | PostgreSQL layer — pgxpool, task result repository |
| `telemetry/telemetry.go` | OpenTelemetry bootstrap — OTLP trace export |
| `go.mod` | Go module definition |
| `docker-compose.yml` | Full stack: Postgres, RabbitMQ, Jaeger, Gateway, Worker×2, Grafana |
| `.github/workflows/ci.yml` | GitHub Actions: lint → test → Docker build |

## Quick Start

```bash
git clone https://github.com/MYSTIC1210/syncflow-distributed-microservices.git
cd syncflow-distributed-microservices

docker compose up --build

# Access points
# Gateway API  → http://localhost:8080
# RabbitMQ UI  → http://localhost:15672  (syncflow / syncflow)
# Jaeger UI    → http://localhost:16686
# Grafana      → http://localhost:3000
```

## Local Dev

```bash
go mod download
go test -race ./...

# Run gateway only (needs Postgres + RabbitMQ already up)
DATABASE_URL=postgres://... RABBITMQ_URL=amqp://... go run .
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | PostgreSQL DSN |
| `RABBITMQ_URL` | — | AMQP connection URL |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | OTLP collector URL |
| `PORT` | `8080` | Gateway listen port |
| `WORKER_COUNT` | `8` | Goroutines per worker replica |

## Worker Pool Design

- `N` goroutines share one `amqp.Channel` with QoS = N (fair dispatch)
- Each delivery: decode → `dispatch()` → handler → `Result` channel
- Success → `Ack`; retriable failure → `Nack(requeue=true)`; max retries exceeded → `Nack(requeue=false)` → DLX
- Results drained to PostgreSQL via `db.SaveTaskResult()`

## CI/CD

GitHub Actions pipeline on every push/PR to `main`:
1. Start Postgres + RabbitMQ service containers
2. `golangci-lint` static analysis
3. `go test -race` with coverage upload to Codecov
4. `docker build` validation

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Go 1.22 |
| Database | PostgreSQL 16 + pgx/v5 |
| Broker | RabbitMQ 3.13 (AMQP 0-9-1) |
| Tracing | OpenTelemetry → Jaeger |
| Metrics | Grafana |
| CI/CD | GitHub Actions |
| Container | Docker + Compose |

## Author

**Dinesh E** — [GitHub](https://github.com/MYSTIC1210) | [LinkedIn](https://www.linkedin.com/in/dinesh-ravilla1210)
