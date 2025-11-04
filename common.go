package migratekit

import (
	"strings"
	"time"
)

const (
	lockTTL        = 5 * time.Minute
	maxRetries     = 40
	retryDelay     = 5 * time.Second
	postgresDriver = "postgres"
	clickhouseDriver = "clickhouse"
)

// Migration is a single SQL migration
type Migration struct {
	Name    string
	Content string
}

// prefix extracts "001" from "001_create_users.up.sql"
func prefix(name string) string {
	name = strings.TrimSuffix(name, ".up.sql")
	name = strings.TrimSuffix(name, ".down.sql")
	if i := strings.IndexByte(name, '_'); i > 0 {
		return name[:i]
	}
	return name
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
