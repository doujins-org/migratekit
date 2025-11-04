package migratekit

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Postgres handles PostgreSQL migrations
type Postgres struct {
	db     *sql.DB
	app    string
	lockID string
}

// NewPostgres creates a Postgres migrator
func NewPostgres(db *sql.DB, app, lockID string) *Postgres {
	return &Postgres{db: db, app: app, lockID: lockID}
}

// Setup ensures migration tables exist (idempotent)
func (p *Postgres) Setup(ctx context.Context) error {
	_, err := p.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS public.migrations (
			id BIGSERIAL PRIMARY KEY,
			app TEXT NOT NULL,
			database TEXT NOT NULL,
			name TEXT NOT NULL,
			migrated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE(app, database, name)
		);
		CREATE TABLE IF NOT EXISTS public.migration_locks (
			id BIGSERIAL PRIMARY KEY,
			app TEXT NOT NULL,
			database TEXT NOT NULL,
			locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			locked_by TEXT NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL,
			UNIQUE(app, database)
		);
	`)
	return err
}

// Applied returns list of applied migration names
func (p *Postgres) Applied(ctx context.Context) ([]string, error) {
	rows, err := p.db.QueryContext(ctx,
		`SELECT name FROM public.migrations WHERE app = $1 AND database = $2 ORDER BY name`,
		p.app, postgresDriver)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// Lock acquires the migration lock (waits if needed)
func (p *Postgres) Lock(ctx context.Context) error {
	for i := 0; i < maxRetries; i++ {
		var lockedBy string
		var expiresAt time.Time
		err := p.db.QueryRowContext(ctx,
			`SELECT locked_by, expires_at FROM public.migration_locks
			 WHERE app = $1 AND database = $2 ORDER BY locked_at DESC LIMIT 1`,
			p.app, postgresDriver).Scan(&lockedBy, &expiresAt)

		if err == sql.ErrNoRows || (err == nil && time.Now().After(expiresAt)) {
			result, err := p.db.ExecContext(ctx,
				`INSERT INTO public.migration_locks (app, database, locked_by, expires_at)
				 VALUES ($1, $2, $3, $4)
				 ON CONFLICT (app, database) DO UPDATE
				 SET locked_by = $3, locked_at = NOW(), expires_at = $4
				 WHERE migration_locks.expires_at < NOW()`,
				p.app, postgresDriver, p.lockID, time.Now().Add(lockTTL))
			if err != nil {
				return err
			}
			if n, _ := result.RowsAffected(); n > 0 {
				return nil
			}
		} else if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
		}
	}
	return fmt.Errorf("lock timeout")
}

// Unlock releases the migration lock
func (p *Postgres) Unlock(ctx context.Context) error {
	_, err := p.db.ExecContext(ctx,
		`DELETE FROM public.migration_locks WHERE app = $1 AND database = $2 AND locked_by = $3`,
		p.app, postgresDriver, p.lockID)
	return err
}

// Apply applies a single migration
func (p *Postgres) Apply(ctx context.Context, m Migration) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, m.Content); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO public.migrations (app, database, name) VALUES ($1, $2, $3)`,
		p.app, postgresDriver, Prefix(m.Name)); err != nil {
		return err
	}

	return tx.Commit()
}

// ApplyMigrations applies all unapplied migrations (only locks if needed)
func (p *Postgres) ApplyMigrations(ctx context.Context, migrations []Migration) error {
	// Check what's already applied (no lock needed)
	applied, err := p.Applied(ctx)
	if err != nil {
		return err
	}

	// Filter to only unapplied migrations
	var toApply []Migration
	for _, mig := range migrations {
		if !contains(applied, Prefix(mig.Name)) {
			toApply = append(toApply, mig)
		}
	}

	if len(toApply) == 0 {
		return nil // Nothing to do, no lock needed
	}

	// Acquire lock only when we have work to do
	if err := p.Lock(ctx); err != nil {
		return err
	}
	defer p.Unlock(ctx)

	// Apply migrations
	for _, mig := range toApply {
		if err := p.Apply(ctx, mig); err != nil {
			return err
		}
	}

	return nil
}

// ValidateAllApplied checks if all provided migrations have been applied.
// Returns an error listing any pending migrations if validation fails.
// This is intended for use during application startup to ensure the database
// schema is up-to-date before the app starts serving requests.
func (p *Postgres) ValidateAllApplied(ctx context.Context, migrations []Migration) error {
	applied, err := p.Applied(ctx)
	if err != nil {
		// If the migrations table doesn't exist, no migrations have been applied
		if strings.Contains(err.Error(), "does not exist") {
			if len(migrations) == 0 {
				return nil // No migrations expected, validation passes
			}
			return fmt.Errorf("migration table does not exist - %d migrations need to be applied", len(migrations))
		}
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}

	// Convert applied list to map for quick lookup
	appliedMap := make(map[string]bool)
	for _, name := range applied {
		appliedMap[name] = true
	}

	// Check which migrations are pending
	var pending []string
	for _, mig := range migrations {
		if !appliedMap[Prefix(mig.Name)] {
			pending = append(pending, mig.Name)
		}
	}

	if len(pending) > 0 {
		return fmt.Errorf("%d pending migrations must be applied: %v", len(pending), pending)
	}

	return nil
}

// Close closes the database connection
func (p *Postgres) Close() error {
	return p.db.Close()
}
