# migratekit

A unified migration library for PostgreSQL and ClickHouse with app-scoped migrations and automatic lock management.

## Features

- ✅ **Unified migration tracking** - Single `public.migrations` table for all apps and databases
- ✅ **App-scoped migrations** - Each app (doujins, hentai0, billing, authkit) has its own migration sequence
- ✅ **Automatic locking** - Per-app locks prevent concurrent migration conflicts
- ✅ **Lock expiration** - Locks auto-expire after 5 minutes to prevent stuck migrations
- ✅ **Wait for locks** - Instead of failing, migrations wait for locks to be released
- ✅ **Numeric prefixes** - Stores only `001`, `002`, etc. (not full filenames)
- ✅ **Transaction safety** - PostgreSQL migrations run in transactions
- ✅ **ClickHouse support** - First-class support for ClickHouse migrations

## Installation

```bash
go get github.com/doujins-org/migratekit
```

## Schema

migratekit uses two tables in the `public` schema (created by bootstrap migrations):

### public.migrations
```sql
CREATE TABLE public.migrations (
    id BIGSERIAL PRIMARY KEY,
    app TEXT NOT NULL,              -- 'doujins', 'hentai0', 'billing', 'authkit'
    database TEXT NOT NULL,         -- 'postgres', 'clickhouse'
    name TEXT NOT NULL,             -- Numeric prefix: '001', '002', '003'
    migrated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(app, database, name)
);
```

### public.migration_locks
```sql
CREATE TABLE public.migration_locks (
    id BIGSERIAL PRIMARY KEY,
    app TEXT NOT NULL,
    database TEXT NOT NULL,
    locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_by TEXT NOT NULL,        -- hostname or pid
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE(app, database)
);
```

## Usage

### PostgreSQL Migrations

```go
package main

import (
    "context"
    "database/sql"

    "github.com/doujins-org/migratekit"
    _ "github.com/lib/pq"
)

func main() {
    ctx := context.Background()

    // Connect to database
    db, _ := sql.Open("postgres", "postgres://user:pass@localhost/dbname")
    defer db.Close()

    // Create migrator
    migrator := migratekit.NewPostgresMigrator(db, "doujins")

    // Ensure migration tables exist (idempotent)
    migrator.InitMigrationTable(ctx)

    // Acquire lock (waits if another instance is running migrations)
    if err := migrator.AcquireLock(ctx); err != nil {
        panic(err)
    }
    defer migrator.ReleaseLock(ctx)

    // Load your migrations
    migrations := []migratekit.Migration{
        {Name: "001_create_users.up.sql", Content: "CREATE TABLE users..."},
        {Name: "002_add_email.up.sql", Content: "ALTER TABLE users..."},
    }

    // Apply migrations (skips already-applied ones)
    if err := migrator.ApplyMigrations(ctx, migrations); err != nil {
        panic(err)
    }
}
```

### ClickHouse Migrations

```go
package main

import (
    "context"

    "github.com/doujins-org/migratekit"
)

func main() {
    ctx := context.Background()

    // Create migrator
    migrator := migratekit.NewClickHouseMigrator(
        "http://localhost:8123",
        "analytics",
        "default",
        "",
        "doujins",
    )

    // Ensure database and tables exist
    migrator.EnsureDatabase(ctx)
    migrator.InitMigrationTable(ctx)

    // Acquire lock
    if err := migrator.AcquireLock(ctx); err != nil {
        panic(err)
    }
    defer migrator.ReleaseLock(ctx)

    // Apply migrations
    migrations := []migratekit.Migration{
        {Name: "001_create_events.up.sql", Content: "CREATE TABLE events..."},
    }

    for _, mig := range migrations {
        if err := migrator.ApplyMigration(ctx, mig); err != nil {
            panic(err)
        }
    }
}
```

## Migration File Format

Migration files follow this naming convention:
```
001_create_users.up.sql
002_add_indexes.up.sql
003_add_timestamps.up.sql
```

Only the numeric prefix (`001`, `002`, etc.) is stored in the database. The descriptive part is for developer readability.

## Environment Variables

- `APP_NAME` - Override the app name (defaults to what you pass to the constructor)

## Lock Behavior

- **Lock TTL**: 5 minutes (prevents stuck locks)
- **Wait behavior**: Up to 200 seconds (40 attempts × 5 seconds)
- **Per-app locking**: `doujins` and `hentai0` can migrate concurrently
- **Per-database locking**: Same app can run Postgres + ClickHouse migrations concurrently

## Design Decisions

### Why store only numeric prefixes?
Matches the pattern used by popular migration tools (like bun) and keeps the database simple. Full filenames are for developers, not the database.

### Why a single migrations table?
- Easy to query: "Show me all migrations across all apps"
- Atomic operations: Single table for all apps
- Simple permissions: Grant once on `public.migrations`

### Why per-app locking?
Different apps (doujins, hentai0, billing) have independent migration sequences and should be able to migrate concurrently.

## License

Private - doujins.org internal use only
