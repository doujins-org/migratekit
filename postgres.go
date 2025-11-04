package migratekit

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// PostgresMigrator handles PostgreSQL migrations
type PostgresMigrator struct {
	DB      *sql.DB
	AppName string
	LockID  string
}

// NewPostgresMigrator creates a new Postgres migrator
// db should already be connected and ready to use
func NewPostgresMigrator(db *sql.DB, appName string) *PostgresMigrator {
	return &PostgresMigrator{
		DB:      db,
		AppName: appName,
		LockID:  GetLockID(),
	}
}

// Close closes the database connection
func (m *PostgresMigrator) Close() error {
	if m.DB != nil {
		return m.DB.Close()
	}
	return nil
}

// InitMigrationTable ensures migration tracking tables exist in public schema
// This is typically called once during bootstrap, but it's safe to call multiple times
func (m *PostgresMigrator) InitMigrationTable(ctx context.Context) error {
	// Create migrations table
	migrationsTable := `
		CREATE TABLE IF NOT EXISTS public.migrations (
			id BIGSERIAL PRIMARY KEY,
			app TEXT NOT NULL,
			database TEXT NOT NULL,
			name TEXT NOT NULL,
			migrated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE(app, database, name)
		)`

	if _, err := m.DB.ExecContext(ctx, migrationsTable); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Create migration locks table
	locksTable := `
		CREATE TABLE IF NOT EXISTS public.migration_locks (
			id BIGSERIAL PRIMARY KEY,
			app TEXT NOT NULL,
			database TEXT NOT NULL,
			locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			locked_by TEXT NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL,
			UNIQUE(app, database)
		)`

	if _, err := m.DB.ExecContext(ctx, locksTable); err != nil {
		return fmt.Errorf("failed to create migration_locks table: %w", err)
	}

	return nil
}

// GetAppliedMigrations returns list of applied migration names for this app
func (m *PostgresMigrator) GetAppliedMigrations(ctx context.Context) ([]string, error) {
	query := `SELECT name FROM public.migrations WHERE app = $1 AND database = 'postgres' ORDER BY name`
	rows, err := m.DB.QueryContext(ctx, query, m.AppName)
	if err != nil {
		return nil, fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	var migrations []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan migration name: %w", err)
		}
		migrations = append(migrations, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating migration rows: %w", err)
	}

	return migrations, nil
}

// AcquireLock attempts to acquire the migration lock with retry logic
func (m *PostgresMigrator) AcquireLock(ctx context.Context) error {
	for attempt := 1; attempt <= MaxLockWaitAttempts; attempt++ {
		// Check for existing lock
		query := `
			SELECT locked_by, expires_at
			FROM public.migration_locks
			WHERE app = $1 AND database = 'postgres'
			ORDER BY locked_at DESC
			LIMIT 1`

		var lockedBy string
		var expiresAt time.Time
		err := m.DB.QueryRowContext(ctx, query, m.AppName).Scan(&lockedBy, &expiresAt)

		now := time.Now()
		canAcquire := false

		if err == sql.ErrNoRows {
			// No lock exists, we can acquire
			canAcquire = true
		} else if err != nil {
			// Some other error (table doesn't exist?)
			return fmt.Errorf("failed to check for existing lock: %w", err)
		} else if now.After(expiresAt) {
			// Lock exists but has expired
			canAcquire = true
			fmt.Printf("Postgres migration lock expired (was held by %s), acquiring...\n", lockedBy)
		}

		if canAcquire {
			// Try to insert our lock (using ON CONFLICT to handle race conditions)
			insertLock := `
				INSERT INTO public.migration_locks (app, database, locked_by, locked_at, expires_at)
				VALUES ($1, 'postgres', $2, NOW(), $3)
				ON CONFLICT (app, database)
				DO UPDATE SET locked_by = $2, locked_at = NOW(), expires_at = $3
				WHERE migration_locks.expires_at < NOW()`

			expiresAt := now.Add(LockTTL)
			result, err := m.DB.ExecContext(ctx, insertLock, m.AppName, m.LockID, expiresAt)
			if err != nil {
				return fmt.Errorf("failed to insert migration lock: %w", err)
			}

			// Check if we actually acquired the lock (rowsAffected > 0)
			rowsAffected, _ := result.RowsAffected()
			if rowsAffected > 0 {
				fmt.Printf("✓ Acquired Postgres migration lock for app=%s (lock_id=%s)\n", m.AppName, m.LockID)
				return nil
			}

			// Someone else acquired it between our check and insert, retry
		}

		// Lock is held by someone else, wait and retry
		if attempt == 1 {
			fmt.Printf("Postgres migration lock for app=%s is held by another instance, waiting...\n", m.AppName)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for migration lock: %w", ctx.Err())
		case <-time.After(LockRetryDelay):
			// Continue to next attempt
		}
	}

	return fmt.Errorf("timed out waiting for Postgres migration lock after %d attempts", MaxLockWaitAttempts)
}

// ReleaseLock releases the migration lock
func (m *PostgresMigrator) ReleaseLock(ctx context.Context) error {
	query := `
		DELETE FROM public.migration_locks
		WHERE app = $1 AND database = 'postgres' AND locked_by = $2`

	_, err := m.DB.ExecContext(ctx, query, m.AppName, m.LockID)
	if err != nil {
		return fmt.Errorf("failed to release migration lock: %w", err)
	}

	fmt.Printf("✓ Released Postgres migration lock for app=%s\n", m.AppName)
	return nil
}

// ApplyMigration applies a single migration
func (m *PostgresMigrator) ApplyMigration(ctx context.Context, migration Migration) error {
	numericName := ExtractNumericPrefix(migration.Name)

	// Start a transaction for the migration
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Will be no-op if tx.Commit() succeeds

	// Execute the migration SQL
	fmt.Printf("Applying migration: %s\n", migration.Name)
	if _, err := tx.ExecContext(ctx, migration.Content); err != nil {
		return fmt.Errorf("failed to execute migration %s: %w", migration.Name, err)
	}

	// Record the migration as applied
	recordQuery := `
		INSERT INTO public.migrations (app, database, name, migrated_at)
		VALUES ($1, 'postgres', $2, NOW())`

	if _, err := tx.ExecContext(ctx, recordQuery, m.AppName, numericName); err != nil {
		return fmt.Errorf("failed to record migration %s: %w", migration.Name, err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit migration %s: %w", migration.Name, err)
	}

	fmt.Printf("✓ Applied migration: %s\n", migration.Name)
	return nil
}

// ApplyMigrations applies multiple migrations in order
// Only applies migrations that haven't been applied yet
func (m *PostgresMigrator) ApplyMigrations(ctx context.Context, migrations []Migration) error {
	// Get already-applied migrations
	applied, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}

	appliedMap := make(map[string]bool)
	for _, name := range applied {
		appliedMap[name] = true
	}

	// Apply pending migrations
	appliedCount := 0
	for _, migration := range migrations {
		numericPrefix := ExtractNumericPrefix(migration.Name)
		if appliedMap[numericPrefix] {
			fmt.Printf("Skipping (already applied): %s\n", migration.Name)
			continue
		}

		if err := m.ApplyMigration(ctx, migration); err != nil {
			return err
		}
		appliedCount++
	}

	if appliedCount == 0 {
		fmt.Println("No pending migrations to apply")
	} else {
		fmt.Printf("✓ Applied %d migration(s)\n", appliedCount)
	}

	return nil
}
