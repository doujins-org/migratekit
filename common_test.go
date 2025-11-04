package migratekit

import "testing"

func TestPrefix(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// Original format with underscores
		{"001_create_users.up.sql", "1"},
		{"002_add_indexes.up.sql", "2"},
		{"042_add_field.up.sql", "42"},

		// Variable number of leading zeros
		{"1_create_users.up.sql", "1"},
		{"01_create_users.up.sql", "1"},
		{"0001_create_users.up.sql", "1"},

		// Hyphen separator
		{"001-create-users.up.sql", "1"},
		{"1-create-users.up.sql", "1"},
		{"42-add-feature.up.sql", "42"},

		// Down migrations
		{"001_rollback.down.sql", "1"},
		{"42-rollback.down.sql", "42"},

		// Edge cases
		{"0_init.up.sql", "0"},
		{"000_init.up.sql", "0"},
		{"100_milestone.up.sql", "100"},

		// No separator (just number)
		{"001.up.sql", "1"},
		{"42.up.sql", "42"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := Prefix(tt.input)
			if result != tt.expected {
				t.Errorf("Prefix(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
