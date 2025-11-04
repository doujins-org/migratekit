# migratekit

Minimal database migration library with app-scoped migrations and automatic locking.

## Install

```bash
go get github.com/doujins-org/migratekit
```

## Usage

### PostgreSQL (Complete Example)

```go
package main

import (
    "context"
    "database/sql"
    "embed"

    "github.com/doujins-org/migratekit"
    _ "github.com/lib/pq"
)

//go:embed migrations/postgres/*.sql
var postgresFS embed.FS

func main() {
    ctx := context.Background()
    db, _ := sql.Open("postgres", "postgres://...")

    // Load migrations from embedded FS
    migrations, _ := migratekit.LoadFromFS(postgresFS, "migrations/postgres")

    // Run migrations (3 lines)
    m := migratekit.NewPostgres(db, "doujins", "pod-123")
    m.Setup(ctx)
    m.ApplyMigrations(ctx, migrations)
}
```

### ClickHouse (Complete Example)

```go
//go:embed migrations/clickhouse/*.sql
var clickhouseFS embed.FS

func main() {
    ctx := context.Background()

    // Load migrations from embedded FS
    migrations, _ := migratekit.LoadFromFS(clickhouseFS, "migrations/clickhouse")

    // Run migrations (3 lines)
    m := migratekit.NewClickHouse(url, db, user, pass, "doujins", "pod-123")
    m.Setup(ctx)
    m.ApplyMigrations(ctx, migrations)
}
```

### Advanced (Manual Control)

```go
m := migratekit.NewPostgres(db, "doujins", "pod-123")
m.Setup(ctx)

applied, _ := m.Applied(ctx)  // Get list of applied migrations (no lock)

// Only lock if you have work to do
var toApply []Migration
for _, mig := range migrations {
    if !contains(applied, prefix(mig.Name)) {
        toApply = append(toApply, mig)
    }
}

if len(toApply) > 0 {
    m.Lock(ctx)
    defer m.Unlock(ctx)
    for _, mig := range toApply {
        m.Apply(ctx, mig)
    }
}
```

## API

### Primary Methods
- `LoadFromFS(fsys, dir)` - Load migrations from embedded filesystem
- `NewPostgres(db, app, lockID)` - Create Postgres migrator
- `NewClickHouse(url, db, user, pass, app, lockID)` - Create ClickHouse migrator
- `Setup(ctx)` - Create migration tables (idempotent)
- `ApplyMigrations(ctx, []Migration)` - Apply all pending migrations (recommended)

### Advanced Methods
- `Lock(ctx)` - Acquire lock (waits up to 200s)
- `Unlock(ctx)` - Release lock
- `Applied(ctx)` - List of applied migration names (`[]string`)
- `Apply(ctx, Migration)` - Apply a single migration
- `Close()` - Cleanup

## Schema

migratekit creates two tables in `public` schema on first `Setup()`:

```sql
CREATE TABLE public.migrations (
    id BIGSERIAL PRIMARY KEY,
    app TEXT NOT NULL,
    database TEXT NOT NULL,
    name TEXT NOT NULL,
    migrated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(app, database, name)
);

CREATE TABLE public.migration_locks (
    id BIGSERIAL PRIMARY KEY,
    app TEXT NOT NULL,
    database TEXT NOT NULL,
    locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_by TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE(app, database)
);
```

## Migration Files

**Naming Convention (Strict)**

Files **must** follow this pattern: `{number}_{description}.up.sql`

✅ **Valid:**
```
001_create_users.up.sql
002_add_indexes.up.sql
003_add_timestamps.up.sql
```

❌ **Invalid (will be skipped):**
```
001_create_users.sql        # Missing .up.sql
1_create_users.up.sql       # Should be 001 (3 digits)
create_users.up.sql         # Missing numeric prefix
001-create-users.up.sql     # Use underscore, not hyphen
```

**Why `.up.sql` is required:**
- Standard convention used by golang-migrate, bun, etc.
- Reserves `.down.sql` for future rollback support
- Prevents accidental execution of non-migration SQL files

**What gets stored:**
Only the numeric prefix (`001`, `002`, `003`) is stored in the database.

## Features

- **Smart locking**: Only locks when there's work to do
- **App-scoped**: Each app has independent migration sequences
- **Auto-expiring locks**: 5-minute TTL prevents stuck locks
- **Wait behavior**: Retries for 200s instead of failing
- **Self-contained**: Creates own tables on first run
- **Minimal**: ~500 lines total, stdlib only

## Design

### Why lock only when needed?
Checking what's applied (`SELECT`) doesn't need a lock. Only write operations need locks. This allows multiple services to check migrations concurrently without blocking.

### Why per-app scoping?
Different apps (doujins, hentai0, billing) have independent migration sequences and can migrate concurrently.

### Why single table?
Easy to query "show all migrations" and simpler permissions.
