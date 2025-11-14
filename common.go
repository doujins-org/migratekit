package migratekit

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	lockTTL          = 30 * time.Second
	maxRetries       = 30
	retryDelay       = 1 * time.Second
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

// isValidEnvVarName checks if a string is a valid environment variable name.
// Valid names contain only uppercase letters, digits, and underscores.
// This prevents substituting SQL constructs like '{}' for empty arrays.
func isValidEnvVarName(name string) bool {
	if name == "" {
		return false
	}
	for _, ch := range name {
		if !(ch >= 'A' && ch <= 'Z') && !(ch >= '0' && ch <= '9') && ch != '_' {
			return false
		}
	}
	return true
}

// substituteTemplates replaces template variables in SQL with environment variable values.
// Template format: {VAR_NAME} -> looks up os.Getenv("VAR_NAME")
// Example: {CLICKHOUSE_PASSWORD} -> os.Getenv("CLICKHOUSE_PASSWORD")
// Only substitutes valid environment variable names (uppercase, digits, underscores).
func substituteTemplates(sql string) string {
	// Find all template variables: {VARIABLE_NAME}
	result := sql
	start := 0
	for {
		openIdx := strings.Index(result[start:], "{")
		if openIdx == -1 {
			break
		}
		openIdx += start

		closeIdx := strings.Index(result[openIdx:], "}")
		if closeIdx == -1 {
			break
		}
		closeIdx += openIdx

		// Extract variable name
		varName := result[openIdx+1 : closeIdx]

		// Only substitute if varName looks like an environment variable
		// This avoids substituting SQL constructs like '{}' for empty arrays
		if !isValidEnvVarName(varName) {
			// Skip this one, move past the closing brace
			start = closeIdx + 1
			continue
		}

		// Get environment variable value
		value := os.Getenv(varName)

		// Replace template with value
		result = result[:openIdx] + value + result[closeIdx+1:]

		// Move start position forward
		start = openIdx + len(value)
	}

	return result
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

// DefaultLockID returns a default lock identifier based on hostname (or PID as fallback).
// This is used by ClickHouse migrations. Postgres migrations use a global advisory lock instead.
func DefaultLockID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return fmt.Sprintf("pid-%d", os.Getpid())
	}
	return hostname
}

// ValidatePostgresMigrations validates multiple Postgres migration sources at once.
// Returns an error if any migrations are pending.
func ValidatePostgresMigrations(ctx context.Context, db *sql.DB, sources ...MigrationSource) error {
	for _, source := range sources {
		migrations, err := LoadFromFS(source.FS)
		if err != nil {
			return fmt.Errorf("failed to load %s migrations: %w", source.App, err)
		}

		migrator := NewPostgres(db, source.App)
		if err := migrator.ValidateAllApplied(ctx, migrations); err != nil {
			return fmt.Errorf("%s migrations not applied: %w", source.App, err)
		}
	}
	return nil
}

// ValidateClickHouseMigrations validates ClickHouse migrations.
// Returns an error if any migrations are pending.
func ValidateClickHouseMigrations(ctx context.Context, config *ClickHouseConfig, fs embed.FS) error {
	migrations, err := LoadFromFS(fs)
	if err != nil {
		return fmt.Errorf("failed to load %s migrations: %w", config.App, err)
	}

	// Override LockID for validation
	validatorConfig := *config
	validatorConfig.LockID = "validator"

	migrator := NewClickHouse(&validatorConfig)
	if err := migrator.ValidateAllApplied(ctx, migrations); err != nil {
		return fmt.Errorf("%s migrations not applied: %w", config.App, err)
	}
	return nil
}
