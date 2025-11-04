package migratekit

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"
	"time"
)

const (
	lockTTL          = 5 * time.Minute
	maxRetries       = 40
	retryDelay       = 5 * time.Second
	postgresDriver   = "postgres"
	clickhouseDriver = "clickhouse"
)

// Migration is a single SQL migration
type Migration struct {
	Name    string
	Content string
}

// Prefix extracts numeric prefix from migration filenames and normalizes it.
// Supports both underscore and hyphen separators.
// Examples:
//
//	"001_create_users.up.sql" -> "1"
//	"1-create-users.up.sql"   -> "1"
//	"0042_add_field.up.sql"   -> "42"
func Prefix(name string) string {
	name = strings.TrimSuffix(name, ".up.sql")
	name = strings.TrimSuffix(name, ".down.sql")

	// Find separator (underscore or hyphen)
	sepIdx := -1
	if i := strings.IndexByte(name, '_'); i > 0 {
		sepIdx = i
	} else if i := strings.IndexByte(name, '-'); i > 0 {
		sepIdx = i
	}

	var numericPart string
	if sepIdx > 0 {
		numericPart = name[:sepIdx]
	} else {
		numericPart = name
	}

	// Normalize by removing leading zeros
	// "001" -> "1", "0042" -> "42", "1" -> "1"
	normalized := strings.TrimLeft(numericPart, "0")
	if normalized == "" {
		// All zeros case: "000" -> "0"
		return "0"
	}
	return normalized
}

// contains checks if a string is in a slice
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// splitSQL splits SQL into statements, removing comments
func splitSQL(sql string) []string {
	sql = strings.ReplaceAll(sql, "\r\n", "\n")
	sql = strings.ReplaceAll(sql, "\r", "\n")

	// Remove block comments
	for {
		if i := strings.Index(sql, "/*"); i >= 0 {
			if j := strings.Index(sql[i+2:], "*/"); j >= 0 {
				sql = sql[:i] + sql[i+j+4:]
			} else {
				sql = sql[:i]
				break
			}
		} else {
			break
		}
	}

	// Remove line comments
	var b strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		if t := strings.TrimSpace(line); !strings.HasPrefix(t, "--") && t != "" {
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}

	// Split by semicolon
	var out []string
	for _, stmt := range strings.Split(b.String(), ";") {
		if s := strings.TrimSpace(stmt); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// MigrationSource represents a migration source with an app name and embedded filesystem
type MigrationSource struct {
	App string
	FS  embed.FS
}

// ValidatePostgresMigrations validates multiple Postgres migration sources at once.
// Returns an error if any migrations are pending.
func ValidatePostgresMigrations(ctx context.Context, db *sql.DB, sources ...MigrationSource) error {
	for _, source := range sources {
		migrations, err := LoadFromFS(source.FS, ".")
		if err != nil {
			return fmt.Errorf("failed to load %s migrations: %w", source.App, err)
		}

		migrator := NewPostgres(db, source.App, "validator")
		if err := migrator.ValidateAllApplied(ctx, migrations); err != nil {
			return fmt.Errorf("%s migrations not applied: %w", source.App, err)
		}
	}
	return nil
}

// ValidateClickHouseMigrations validates ClickHouse migrations.
// Returns an error if any migrations are pending.
func ValidateClickHouseMigrations(ctx context.Context, serverURL, database, user, pass, app string, fs embed.FS) error {
	migrations, err := LoadFromFS(fs, ".")
	if err != nil {
		return fmt.Errorf("failed to load %s migrations: %w", app, err)
	}

	migrator := NewClickHouse(serverURL, database, user, pass, app, "validator")
	if err := migrator.ValidateAllApplied(ctx, migrations); err != nil {
		return fmt.Errorf("%s migrations not applied: %w", app, err)
	}
	return nil
}
