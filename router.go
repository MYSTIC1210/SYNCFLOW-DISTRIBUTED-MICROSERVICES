// internal/api/router.go — HTTP router for the SyncFlow gateway.
// Registers all routes, middleware (logging, tracing, recovery).
package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/MYSTIC1210/syncflow/internal/broker"
	"github.com/MYSTIC1210/syncflow/db"
)

type router struct {
	mux  *http.ServeMux
	pool *pgxpool.Pool
	mq   *broker.Connection
}

// NewRouter wires up all HTTP routes and returns an http.Handler.
func NewRouter(pool *pgxpool.Pool, mq *broker.Connection) http.Handler {
	r := &router{
		mux:  http.NewServeMux(),
		pool: pool,
		mq:   mq,
	}
	r.routes()
	return logging(tracing(r.mux))
}

func (r *router) routes() {
	r.mux.HandleFunc("GET /health",        r.health)
	r.mux.HandleFunc("POST /tasks",        r.createTask)
	r.mux.HandleFunc("GET /tasks",         r.listTasks)
	r.mux.HandleFunc("GET /tasks/{id}",    r.getTask)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

func (r *router) health(w http.ResponseWriter, req *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
}

type createTaskRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func (r *router) createTask(w http.ResponseWriter, req *http.Request) {
	var body createTaskRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if body.Type == "" {
		http.Error(w, "type is required", http.StatusBadRequest)
		return
	}

	taskID, err := r.mq.PublishTask(req.Context(), body.Type, body.Payload)
	if err != nil {
		log.Printf("[router] publish failed: %v", err)
		http.Error(w, "failed to enqueue task", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"task_id": taskID, "status": "queued"})
}

func (r *router) listTasks(w http.ResponseWriter, req *http.Request) {
	rows, err := db.ListTaskResults(req.Context(), r.pool, 50)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, rows)
}

func (r *router) getTask(w http.ResponseWriter, req *http.Request) {
	id := req.PathValue("id")
	rows, err := db.ListTaskResults(req.Context(), r.pool, 1000)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	for _, row := range rows {
		if row.ID == id {
			writeJSON(w, http.StatusOK, row)
			return
		}
	}
	http.Error(w, "not found", http.StatusNotFound)
}

// ── Middleware ────────────────────────────────────────────────────────────────

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func tracing(next http.Handler) http.Handler {
	tr := otel.Tracer("syncflow-router")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tr.Start(r.Context(), r.Method+" "+r.URL.Path)
		span.SetAttributes(attribute.String("http.method", r.Method), attribute.String("http.path", r.URL.Path))
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
