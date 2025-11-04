package migratekit

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

// Common constants
const (
	// LockTTL is how long a migration lock is valid before it expires
	LockTTL = 5 * time.Minute

	// MaxLockWaitAttempts is the maximum number of times to retry acquiring a lock
	MaxLockWaitAttempts = 40 // 40 attempts * 5 seconds = 200 seconds max wait

	// LockRetryDelay is how long to wait between lock acquisition attempts
	LockRetryDelay = 5 * time.Second
)

// Migration represents a single migration file
type Migration struct {
	Name    string // Full filename (e.g., "001_create_users.up.sql")
	Content string // SQL content
}

// Migrator is the common interface for all database migrators
type Migrator interface {
	// AcquireLock attempts to acquire the migration lock with retry logic
	AcquireLock(ctx context.Context) error

	// ReleaseLock releases the migration lock
	ReleaseLock(ctx context.Context) error

	// GetAppliedMigrations returns a list of already-applied migration names (numeric prefixes)
	GetAppliedMigrations(ctx context.Context) ([]string, error)

	// ApplyMigration applies a single migration
	ApplyMigration(ctx context.Context, migration Migration) error

	// Close closes any underlying connections
	Close() error
}

// ExtractNumericPrefix extracts the numeric prefix from a migration filename
// e.g., "001_create_users.up.sql" -> "001"
func ExtractNumericPrefix(filename string) string {
	// Remove .up.sql or .down.sql suffix
	name := strings.TrimSuffix(filename, ".up.sql")
	name = strings.TrimSuffix(name, ".down.sql")

	// Find the first underscore
	if idx := strings.IndexByte(name, '_'); idx > 0 {
		return name[:idx]
	}

	// No underscore found, return the whole name (might already be numeric)
	return name
}

// GetLockID generates a lock identifier for the current process
// Returns hostname or pid-based identifier
func GetLockID() string {
	lockID, err := os.Hostname()
	if err != nil || lockID == "" {
		lockID = fmt.Sprintf("pid-%d", os.Getpid())
	}
	return lockID
}

// GetAppName returns the app name from environment or a default
func GetAppName(defaultName string) string {
	appName := os.Getenv("APP_NAME")
	if appName == "" {
		appName = defaultName
	}
	return appName
}
