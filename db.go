// db/db.go — PostgreSQL connection pool and repository helpers.
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect returns a pgxpool configured for production use.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}

	cfg.MaxConns           = 20
	cfg.MinConns           = 4
	cfg.MaxConnLifetime    = 30 * time.Minute
	cfg.MaxConnIdleTime    = 5 * time.Minute
	cfg.HealthCheckPeriod  = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

// ── Task repository ────────────────────────────────────────────────────────────

type TaskRow struct {
	ID        string
	Type      string
	Status    string
	Output    string
	DurationMs int64
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func SaveTaskResult(ctx context.Context, pool *pgxpool.Pool, r TaskRow) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO task_results (id, type, status, output, duration_ms, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
		ON CONFLICT (id) DO UPDATE
		  SET status = EXCLUDED.status,
		      output = EXCLUDED.output,
		      duration_ms = EXCLUDED.duration_ms,
		      updated_at = NOW()
	`, r.ID, r.Type, r.Status, r.Output, r.DurationMs, r.CreatedAt)
	return err
}

func ListTaskResults(ctx context.Context, pool *pgxpool.Pool, limit int) ([]TaskRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, type, status, output, duration_ms, created_at, updated_at
		FROM task_results
		ORDER BY created_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TaskRow
	for rows.Next() {
		var r TaskRow
		if err := rows.Scan(&r.ID, &r.Type, &r.Status, &r.Output, &r.DurationMs, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}
