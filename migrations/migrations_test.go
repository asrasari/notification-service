package migrations_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/asrasari/notification-service/migrations"
)

// TestUpDownUp applies every migration, rolls all of them back, and applies
// them again (D-013). It proves each Down section undoes its Up section.
//
// It needs a disposable Postgres database: the test drops every table it
// knows about. Set TEST_DATABASE_URL to run it; it is skipped otherwise.
func TestUpDownUp(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping migration integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	p, err := migrations.NewProvider(db)
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	sources := p.ListSources()
	if len(sources) == 0 {
		t.Fatal("no migrations embedded")
	}
	latest := sources[len(sources)-1].Version

	// Start from an empty schema whatever state a previous run left behind.
	if _, err := p.DownTo(ctx, 0); err != nil {
		t.Fatalf("initial down: %v", err)
	}

	up := func(step string) {
		t.Helper()
		if _, err := p.Up(ctx); err != nil {
			t.Fatalf("%s: up: %v", step, err)
		}
		v, err := p.GetDBVersion(ctx)
		if err != nil {
			t.Fatalf("%s: version: %v", step, err)
		}
		if v != latest {
			t.Fatalf("%s: version %d, want %d", step, v, latest)
		}
		for _, table := range []string{"notifications", "delivery_attempts"} {
			if !tableExists(ctx, t, db, table) {
				t.Fatalf("%s: table %s missing after up", step, table)
			}
		}
	}

	up("first up")

	if _, err := p.DownTo(ctx, 0); err != nil {
		t.Fatalf("down: %v", err)
	}
	if v, err := p.GetDBVersion(ctx); err != nil || v != 0 {
		t.Fatalf("after down: version %d, err %v; want 0", v, err)
	}
	for _, table := range []string{"notifications", "delivery_attempts"} {
		if tableExists(ctx, t, db, table) {
			t.Fatalf("table %s still exists after down", table)
		}
	}

	up("second up")
}

func tableExists(ctx context.Context, t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	err := db.QueryRowContext(ctx, `SELECT to_regclass('public.' || $1) IS NOT NULL`, name).Scan(&exists)
	if err != nil {
		t.Fatalf("check table %s: %v", name, err)
	}
	return exists
}
