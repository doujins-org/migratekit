package migratekit

import (
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

// LoadFromFS loads migrations from an embedded filesystem.
// Reads all .up.sql files, sorted by name.
// If dir is empty, defaults to "." (root of the filesystem).
func LoadFromFS(fsys fs.FS, dir ...string) ([]Migration, error) {
	directory := "."
	if len(dir) > 0 && dir[0] != "" {
		directory = dir[0]
	}
	entries, err := fs.ReadDir(fsys, directory)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", directory, err)
	}

	var migrations []Migration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}

		// Construct path correctly for embed.FS
		// embed.FS doesn't accept "./" prefix, so handle "." specially
		var path string
		if directory == "." {
			path = entry.Name()
		} else {
			path = directory + "/" + entry.Name()
		}

		content, err := fs.ReadFile(fsys, path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}

		// Apply template substitution to migration content
		processedContent := substituteTemplates(string(content))

		migrations = append(migrations, Migration{
			Name:    entry.Name(),
			Content: processedContent,
		})
	}

	// Sort by filename (001_, 002_, etc.)
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Name < migrations[j].Name
	})

	return migrations, nil
}
