# migratekit

Minimal database migration library with app-scoped migrations and automatic locking.

## Install

```bash
go get github.com/doujins-org/migratekit
```

## Usage

### PostgreSQL (Recommended - 3 lines)

```go
import "github.com/doujins-org/migratekit"

db, _ := sql.Open("postgres", "...")
m := migratekit.NewPostgres(db, "doujins", "pod-123")

m.Setup(ctx)                        // Create tables (idempotent)
m.ApplyMigrations(ctx, migrations)  // Apply all pending (locks only if needed)
```

### ClickHouse (Recommended - 3 lines)

```go
m := migratekit.NewClickHouse(url, db, user, pass, "doujins", "pod-123")

m.Setup(ctx)                        // Create tables (idempotent)
m.ApplyMigrations(ctx, migrations)  // Apply all pending (locks only if needed)
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

Name migrations with numeric prefix:
```
001_create_users.up.sql
002_add_indexes.up.sql
003_add_timestamps.up.sql
```

Only the numeric prefix (`001`, `002`) is stored in the database.

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
