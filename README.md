# migratekit

Minimal database migration library with app-scoped migrations and automatic locking.

## Install

```bash
go get github.com/doujins-org/migratekit
```

## Usage

### PostgreSQL

```go
import "github.com/doujins-org/migratekit"

db, _ := sql.Open("postgres", "...")
m := migratekit.NewPostgres(db, "doujins", "pod-123")

m.Setup(ctx)           // Create tables (idempotent)
m.Lock(ctx)            // Acquire lock (waits if busy)
defer m.Unlock(ctx)

applied, _ := m.Applied(ctx)
for _, mig := range migrations {
    if !contains(applied, prefix(mig.Name)) {
        m.Apply(ctx, mig)
    }
}
```

### ClickHouse

```go
m := migratekit.NewClickHouse(url, db, user, pass, "doujins", "pod-123")

m.Setup(ctx)
m.Lock(ctx)
defer m.Unlock(ctx)

applied, _ := m.Applied(ctx)
for _, mig := range migrations {
    if !contains(applied, prefix(mig.Name)) {
        m.Apply(ctx, mig)
    }
}
```

## API

Both `Postgres` and `ClickHouse` provide:

- `Setup(ctx)` - Create migration tables
- `Lock(ctx)` - Acquire lock (waits up to 200s)
- `Unlock(ctx)` - Release lock
- `Applied(ctx)` - List of applied migration names (`[]string`)
- `Apply(ctx, Migration)` - Apply a migration
- `Close()` - Cleanup

## Schema

```sql
-- Single table for all apps/databases
CREATE TABLE public.migrations (
    app TEXT,       -- doujins, hentai0, billing
    database TEXT,  -- postgres, clickhouse
    name TEXT,      -- 001, 002, 003
    migrated_at TIMESTAMPTZ,
    UNIQUE(app, database, name)
);

CREATE TABLE public.migration_locks (
    app TEXT,
    database TEXT,
    locked_by TEXT,
    expires_at TIMESTAMPTZ,
    UNIQUE(app, database)
);
```

## Features

- App-scoped: Each app has its own migration sequence
- Auto-expiring locks: 5-minute TTL prevents stuck locks
- Wait behavior: Retries for 200s instead of failing
- Minimal: ~200 lines total, no dependencies except stdlib
