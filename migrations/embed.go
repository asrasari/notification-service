// Package migrations holds the versioned SQL schema (D-013). The files are
// embedded so the migrate binary in the shared image carries them.
package migrations

import (
	"database/sql"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// FS contains every migration file.
//
//go:embed *.sql
var FS embed.FS

// NewProvider returns a goose provider over db with the embedded migrations.
// A Postgres session-level advisory lock makes concurrent runs safe: a second
// migrate process waits instead of applying the same version twice.
func NewProvider(db *sql.DB) (*goose.Provider, error) {
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, fmt.Errorf("migrations: create session locker: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, FS, goose.WithSessionLocker(locker))
	if err != nil {
		return nil, fmt.Errorf("migrations: create provider: %w", err)
	}
	return p, nil
}
