package migratekit

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ClickHouseConfig holds configuration for ClickHouse migrations
type ClickHouseConfig struct {
	ServerURL string
	Database  string
	Username  string
	Password  string
	App       string
	LockID    string // Optional; uses DefaultLockID() if empty
}

// ClickHouse handles ClickHouse migrations via HTTP
type ClickHouse struct {
	client *http.Client
	url    string
	db     string
	user   string
	pass   string
	app    string
	lockID string
}

// NewClickHouse creates a ClickHouse migrator from config.
// If config.LockID is empty, uses DefaultLockID().
func NewClickHouse(config *ClickHouseConfig) *ClickHouse {
	lockID := config.LockID
	if lockID == "" {
		lockID = DefaultLockID()
	}

	return &ClickHouse{
		client: &http.Client{Timeout: 120 * time.Second},
		url:    strings.TrimSuffix(config.ServerURL, "/"),
		db:     config.Database,
		user:   config.Username,
		pass:   config.Password,
		app:    config.App,
		lockID: lockID,
	}
}

// exec executes SQL
func (c *ClickHouse) exec(ctx context.Context, sql string) error {
	endpoint := c.url + "?wait_end_of_query=1"
	if c.db != "" {
		endpoint += "&database=" + url.QueryEscape(c.db)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(sql))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	if c.user != "" && c.pass != "" {
		req.SetBasicAuth(c.user, c.pass)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse: %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// query returns first column as strings
func (c *ClickHouse) query(ctx context.Context, sql string) ([]string, error) {
	endpoint := c.url + "?wait_end_of_query=1"
	if c.db != "" {
		endpoint += "&database=" + url.QueryEscape(c.db)
	}

	sql = strings.TrimSpace(sql)
	if !strings.Contains(strings.ToUpper(sql), "FORMAT") {
		sql = strings.TrimSuffix(sql, ";") + " FORMAT TabSeparated"
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain")
	if c.user != "" && c.pass != "" {
		req.SetBasicAuth(c.user, c.pass)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("clickhouse: %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var out []string
	for _, line := range strings.Split(string(body), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			if i := strings.IndexByte(line, '\t'); i >= 0 {
				out = append(out, line[:i])
			} else {
				out = append(out, line)
			}
		}
	}
	return out, nil
}

// Setup ensures database and tables exist
func (c *ClickHouse) Setup(ctx context.Context) error {
	// NOTE: Database creation is handled by bootstrap migrations, not by migratekit.
	// Bootstrap migrations run as default user with CREATE DATABASE permissions.
	// App migrations run as analytics_user which only has permissions within the analytics database.

	// Create migrations table
	if err := c.exec(ctx, `
		CREATE TABLE IF NOT EXISTS migrations (
			app String,
			name String,
			migrated_at DateTime DEFAULT now()
		) ENGINE = ReplacingMergeTree(migrated_at) ORDER BY (app, name)
	`); err != nil {
		return err
	}

	// Create migration_locks table
	// Note: Uses a global lock (lock_name='global') so only ONE app can migrate at a time
	// The 'app' field records which app acquired the lock (for debugging)
	return c.exec(ctx, `
		CREATE TABLE IF NOT EXISTS migration_locks (
			lock_name String,
			app String,
			locked_at DateTime DEFAULT now(),
			locked_by String,
			expires_at DateTime
		) ENGINE = ReplacingMergeTree(locked_at) ORDER BY lock_name
	`)
}

// Applied returns list of applied migrations
func (c *ClickHouse) Applied(ctx context.Context) ([]string, error) {
	sql := fmt.Sprintf("SELECT name FROM migrations WHERE app = '%s' ORDER BY name",
		strings.ReplaceAll(c.app, "'", "''"))
	rows, err := c.query(ctx, sql)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// Lock acquires a global database-wide migration lock
// All apps share the same lock (lock_name='global') to prevent concurrent ClickHouse migrations
// This is necessary because ON CLUSTER operations modify distributed DDL queue across all nodes
func (c *ClickHouse) Lock(ctx context.Context) error {
	for i := 0; i < maxRetries; i++ {
		// Check global lock (not per-app)
		sql := `SELECT app, locked_by, expires_at FROM migration_locks
			WHERE lock_name = 'global' ORDER BY locked_at DESC LIMIT 1`
		rows, _ := c.query(ctx, sql)

		canAcquire := len(rows) == 0
		if len(rows) > 0 {
			parts := strings.Split(rows[0], "\t")
			// parts[0] = app, parts[1] = locked_by, parts[2] = expires_at
			if len(parts) >= 3 {
				if t, err := time.Parse("2006-01-02 15:04:05", parts[2]); err == nil && time.Now().After(t) {
					canAcquire = true
				}
			}
		}

		if canAcquire {
			// Acquire global lock, but record which app acquired it
			sql := fmt.Sprintf(`INSERT INTO migration_locks (lock_name, app, locked_by, locked_at, expires_at)
				VALUES ('global', '%s', '%s', now(), '%s')`,
				strings.ReplaceAll(c.app, "'", "''"),
				strings.ReplaceAll(c.lockID, "'", "''"),
				time.Now().Add(lockTTL).Format("2006-01-02 15:04:05"))
			if err := c.exec(ctx, sql); err != nil {
				return err
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
		}
	}
	return fmt.Errorf("lock timeout")
}

// Unlock releases the global lock
func (c *ClickHouse) Unlock(ctx context.Context) error {
	// Delete global lock where this app/lockID acquired it
	sql := fmt.Sprintf("DELETE FROM migration_locks WHERE lock_name = 'global' AND app = '%s' AND locked_by = '%s'",
		strings.ReplaceAll(c.app, "'", "''"),
		strings.ReplaceAll(c.lockID, "'", "''"))
	return c.exec(ctx, sql)
}

// Apply applies a migration
func (c *ClickHouse) Apply(ctx context.Context, m Migration) error {
	for _, stmt := range splitSQL(m.Content) {
		if err := c.exec(ctx, stmt); err != nil {
			return err
		}
	}

	// Check if migration already recorded (handles concurrent migrations)
	checkSQL := fmt.Sprintf("SELECT count(*) FROM migrations WHERE app = '%s' AND name = '%s'",
		strings.ReplaceAll(c.app, "'", "''"),
		strings.ReplaceAll(Prefix(m.Name), "'", "''"))

	rows, err := c.query(ctx, checkSQL)
	if err != nil {
		return err
	}

	// If already recorded by another app, skip insertion
	if len(rows) > 0 && rows[0] != "0" {
		return nil
	}

	sql := fmt.Sprintf("INSERT INTO migrations (app, name) VALUES ('%s', '%s')",
		strings.ReplaceAll(c.app, "'", "''"),
		strings.ReplaceAll(Prefix(m.Name), "'", "''"))
	return c.exec(ctx, sql)
}

// ApplyMigrations applies all unapplied migrations (only locks if needed)
// Automatically calls Setup() to ensure migration tables exist before proceeding.
func (c *ClickHouse) ApplyMigrations(ctx context.Context, migrations []Migration) error {
	// Ensure migration tables exist (must happen before checking applied migrations)
	// This runs outside the lock initially to allow concurrent readers
	applied, err := c.Applied(ctx)
	if err != nil {
		// If migrations table doesn't exist, set up first (CREATE TABLE IF NOT EXISTS is safe for concurrent execution)
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "UNKNOWN_TABLE") {
			// Create the tables first using IF NOT EXISTS (safe for concurrent execution)
			if err := c.Setup(ctx); err != nil {
				return err
			}

			// Now acquire lock to apply migrations
			if err := c.Lock(ctx); err != nil {
				return err
			}
			defer c.Unlock(ctx)

			// After setup, check applied again (still under lock)
			applied, err = c.Applied(ctx)
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

			// Apply migrations (still under lock from setup)
			for _, mig := range toApply {
				if err := c.Apply(ctx, mig); err != nil {
					return err
				}
			}

			return nil
		}
		return err
	}

    // Normal path: tables exist, check what needs to be applied
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
    if err := c.Lock(ctx); err != nil {
        return err
    }
    defer c.Unlock(ctx)

    // Double-check under lock in case another process applied some since our first read
    applied, err = c.Applied(ctx)
    if err != nil {
        return err
    }
    toApply = toApply[:0]
    for _, mig := range migrations {
        if !contains(applied, Prefix(mig.Name)) {
            toApply = append(toApply, mig)
        }
    }
    if len(toApply) == 0 {
        return nil
    }

    // Apply migrations
    for _, mig := range toApply {
        if err := c.Apply(ctx, mig); err != nil {
            return err
        }
    }

    return nil
}

// ValidateAllApplied checks if all provided migrations have been applied.
// Returns an error listing any pending migrations if validation fails.
// This is intended for use during application startup to ensure the database
// schema is up-to-date before the app starts serving requests.
func (c *ClickHouse) ValidateAllApplied(ctx context.Context, migrations []Migration) error {
	applied, err := c.Applied(ctx)
	if err != nil {
		// If query fails, assume table doesn't exist and no migrations applied
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "Unknown table") {
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

// Close is a no-op for HTTP client
func (c *ClickHouse) Close() error {
	return nil
}
