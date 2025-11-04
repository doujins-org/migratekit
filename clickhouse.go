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

// ClickHouseMigrator handles ClickHouse migrations using HTTP interface
type ClickHouseMigrator struct {
	Client    *http.Client
	ServerURL string
	Database  string
	Username  string
	Password  string
	AppName   string
	LockID    string
}

// NewClickHouseMigrator creates a new ClickHouse migrator
func NewClickHouseMigrator(serverURL, database, username, password, appName string) *ClickHouseMigrator {
	return &ClickHouseMigrator{
		Client:    &http.Client{Timeout: 30 * time.Second},
		ServerURL: strings.TrimSuffix(serverURL, "/"),
		Database:  database,
		Username:  username,
		Password:  password,
		AppName:   appName,
		LockID:    GetLockID(),
	}
}

// Close closes any underlying connections (no-op for ClickHouse HTTP client)
func (m *ClickHouseMigrator) Close() error {
	return nil
}

// ExecuteSQL executes a SQL query against ClickHouse
func (m *ClickHouseMigrator) ExecuteSQL(ctx context.Context, query string) error {
	endpoint := m.ServerURL
	// Always request to wait for end-of-query before sending HTTP headers
	if strings.Contains(endpoint, "?") {
		endpoint += "&wait_end_of_query=1"
	} else {
		endpoint += "?wait_end_of_query=1"
	}
	if m.Database != "" {
		endpoint += "&database=" + url.QueryEscape(m.Database)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(query))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if m.Username != "" || m.Password != "" {
		req.SetBasicAuth(m.Username, m.Password)
	}

	resp, err := m.Client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		bodyStr := strings.TrimSpace(string(body))
		if bodyStr != "" {
			return fmt.Errorf("ClickHouse returned status %d: %s", resp.StatusCode, bodyStr)
		}
		return fmt.Errorf("ClickHouse returned status %d", resp.StatusCode)
	}

	return nil
}

// QueryStrings executes a SELECT and returns the first column as []string
func (m *ClickHouseMigrator) QueryStrings(ctx context.Context, query string) ([]string, error) {
	endpoint := m.ServerURL
	if strings.Contains(endpoint, "?") {
		endpoint += "&wait_end_of_query=1"
	} else {
		endpoint += "?wait_end_of_query=1"
	}
	if m.Database != "" {
		endpoint += "&database=" + url.QueryEscape(m.Database)
	}

	q := strings.TrimSpace(query)
	if !strings.Contains(strings.ToUpper(q), "FORMAT ") {
		if strings.HasSuffix(q, ";") {
			q = strings.TrimSuffix(q, ";")
		}
		q += " FORMAT TabSeparated"
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(q))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if m.Username != "" || m.Password != "" {
		req.SetBasicAuth(m.Username, m.Password)
	}

	resp, err := m.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		bodyStr := strings.TrimSpace(string(body))
		if bodyStr != "" {
			return nil, fmt.Errorf("ClickHouse returned status %d: %s", resp.StatusCode, bodyStr)
		}
		return nil, fmt.Errorf("ClickHouse returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	var out []string
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Only first column is needed; TabSeparated means columns are split by '\t'
		if idx := strings.IndexByte(line, '\t'); idx >= 0 {
			out = append(out, line[:idx])
		} else {
			out = append(out, line)
		}
	}
	return out, nil
}

// EnsureDatabase ensures the target database exists
func (m *ClickHouseMigrator) EnsureDatabase(ctx context.Context) error {
	if m.Database == "" {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, "POST", m.ServerURL, strings.NewReader(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", m.Database)))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if m.Username != "" || m.Password != "" {
		req.SetBasicAuth(m.Username, m.Password)
	}
	resp, err := m.Client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		bodyStr := strings.TrimSpace(string(body))
		if bodyStr != "" {
			return fmt.Errorf("ClickHouse returned status %d while creating database: %s", resp.StatusCode, bodyStr)
		}
		return fmt.Errorf("ClickHouse returned status %d while creating database", resp.StatusCode)
	}
	return nil
}

// InitMigrationTable creates the migration tracking tables
func (m *ClickHouseMigrator) InitMigrationTable(ctx context.Context) error {
	migrationsTable := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			app String,
			name String,
			migrated_at DateTime DEFAULT now()
		)
		ENGINE = ReplacingMergeTree(migrated_at)
		ORDER BY (app, name)`

	if err := m.ExecuteSQL(ctx, migrationsTable); err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	locksTable := `
		CREATE TABLE IF NOT EXISTS schema_migration_locks (
			app String,
			locked_at DateTime DEFAULT now(),
			locked_by String,
			expires_at DateTime
		)
		ENGINE = ReplacingMergeTree(locked_at)
		ORDER BY app`

	if err := m.ExecuteSQL(ctx, locksTable); err != nil {
		return fmt.Errorf("failed to create schema_migration_locks table: %w", err)
	}

	return nil
}

// GetAppliedMigrations returns list of applied migration names for this app
func (m *ClickHouseMigrator) GetAppliedMigrations(ctx context.Context) ([]string, error) {
	query := fmt.Sprintf("SELECT name FROM schema_migrations WHERE app = '%s' ORDER BY name",
		strings.ReplaceAll(m.AppName, "'", "''"))
	rows, err := m.QueryStrings(ctx, query)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// AcquireLock attempts to acquire the migration lock with retry logic
func (m *ClickHouseMigrator) AcquireLock(ctx context.Context) error {
	for attempt := 1; attempt <= MaxLockWaitAttempts; attempt++ {
		// Check for existing lock
		query := fmt.Sprintf(`
			SELECT locked_by, expires_at
			FROM schema_migration_locks
			WHERE app = '%s'
			ORDER BY locked_at DESC
			LIMIT 1`,
			strings.ReplaceAll(m.AppName, "'", "''"))

		rows, err := m.QueryStrings(ctx, query)
		if err != nil {
			// Table might not exist yet, try to acquire
			rows = []string{}
		}

		now := time.Now()
		canAcquire := false

		if len(rows) == 0 {
			canAcquire = true
		} else {
			// Parse the lock info: "locked_by\texpires_at"
			parts := strings.Split(rows[0], "\t")
			if len(parts) >= 2 {
				expiresAt, parseErr := time.Parse("2006-01-02 15:04:05", parts[1])
				if parseErr == nil && now.After(expiresAt) {
					canAcquire = true
					fmt.Printf("ClickHouse migration lock expired (was held by %s), acquiring...\n", parts[0])
				}
			}
		}

		if canAcquire {
			expiresAt := now.Add(LockTTL).Format("2006-01-02 15:04:05")
			insertLock := fmt.Sprintf(`
				INSERT INTO schema_migration_locks (app, locked_by, locked_at, expires_at)
				VALUES ('%s', '%s', now(), '%s')`,
				strings.ReplaceAll(m.AppName, "'", "''"),
				strings.ReplaceAll(m.LockID, "'", "''"),
				expiresAt)

			if err := m.ExecuteSQL(ctx, insertLock); err != nil {
				return fmt.Errorf("failed to insert migration lock: %w", err)
			}

			fmt.Printf("✓ Acquired ClickHouse migration lock for app=%s (lock_id=%s)\n", m.AppName, m.LockID)
			return nil
		}

		// Lock is held, wait and retry
		if attempt == 1 {
			fmt.Printf("ClickHouse migration lock for app=%s is held by another instance, waiting...\n", m.AppName)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for migration lock: %w", ctx.Err())
		case <-time.After(LockRetryDelay):
			// Continue to next attempt
		}
	}

	return fmt.Errorf("timed out waiting for ClickHouse migration lock after %d attempts", MaxLockWaitAttempts)
}

// ReleaseLock releases the migration lock
func (m *ClickHouseMigrator) ReleaseLock(ctx context.Context) error {
	query := fmt.Sprintf("DELETE FROM schema_migration_locks WHERE app = '%s' AND locked_by = '%s'",
		strings.ReplaceAll(m.AppName, "'", "''"),
		strings.ReplaceAll(m.LockID, "'", "''"))

	if err := m.ExecuteSQL(ctx, query); err != nil {
		return fmt.Errorf("failed to release migration lock: %w", err)
	}

	fmt.Printf("✓ Released ClickHouse migration lock for app=%s\n", m.AppName)
	return nil
}

// ApplyMigration applies a single migration
func (m *ClickHouseMigrator) ApplyMigration(ctx context.Context, migration Migration) error {
	numericName := ExtractNumericPrefix(migration.Name)

	// Split content into individual statements
	stmts := splitSQLStatements(migration.Content)
	for _, stmt := range stmts {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}

		preview := trimmed
		if nl := strings.IndexByte(preview, '\n'); nl >= 0 {
			preview = preview[:nl]
		}
		if len(preview) > 120 {
			preview = preview[:120] + "..."
		}
		fmt.Printf("Executing: %s\n", preview)
		if err := m.ExecuteSQL(ctx, trimmed); err != nil {
			return fmt.Errorf("failed to execute statement in %s: %w", migration.Name, err)
		}
	}

	// Record migration as applied (app-scoped, numeric prefix only)
	ins := fmt.Sprintf("INSERT INTO schema_migrations (app, name) VALUES ('%s', '%s')",
		strings.ReplaceAll(m.AppName, "'", "''"),
		strings.ReplaceAll(numericName, "'", "''"))
	if err := m.ExecuteSQL(ctx, ins); err != nil {
		return fmt.Errorf("failed to record applied migration %s: %w", migration.Name, err)
	}
	return nil
}

// splitSQLStatements splits SQL text into statements by ';' while removing comments
func splitSQLStatements(sql string) []string {
	// Remove CR to normalize newlines
	s := strings.ReplaceAll(sql, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	// Strip block comments /* ... */
	for {
		start := strings.Index(s, "/*")
		if start < 0 {
			break
		}
		end := strings.Index(s[start+2:], "*/")
		if end < 0 {
			s = s[:start]
			break
		}
		s = s[:start] + s[start+2+end+2:]
	}
	// Strip line comments starting with -- until end of line
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "--") || t == "" {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	cleaned := b.String()
	// Now split by ';'
	raw := strings.Split(cleaned, ";")
	var out []string
	for _, part := range raw {
		stmt := strings.TrimSpace(part)
		if stmt == "" {
			continue
		}
		out = append(out, stmt)
	}
	return out
}
