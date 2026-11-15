package migrations_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/asrasari/notification-service/internal/testdb"
	"github.com/asrasari/notification-service/migrations"
)

func TestMain(m *testing.M) { testdb.Main(m) }

// TestUpDownUp applies every migration, rolls all of them back, and applies
// them again (D-013). It proves each Down section undoes its Up section.
//
// It drops every table, so it runs in its own database from testdb (D-017).
func TestUpDownUp(t *testing.T) {
	dsn := testdb.New(t)

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

	// testdb databases arrive migrated; start from an empty schema instead.
	if _, err := p.DownTo(ctx, 0); err != nil {
		t.Fatalf("initial down: %v", err)
	}

	tables := []string{"notifications", "delivery_attempts"}
	listIndexes := []string{"notifications_created_idx", "notifications_status_created_idx"} // 00002
	heartbeats := "worker_heartbeats"                                                        // 00003

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
		for _, name := range append(append(tables, listIndexes...), heartbeats) {
			if !relationExists(ctx, t, db, name) {
				t.Fatalf("%s: %s missing after up", step, name)
			}
		}
	}

	up("first up")

	// One step down removes only 00003's table.
	if _, err := p.DownTo(ctx, 2); err != nil {
		t.Fatalf("down to 2: %v", err)
	}
	if relationExists(ctx, t, db, heartbeats) {
		t.Fatalf("%s still exists after down to 2", heartbeats)
	}
	for _, name := range append(tables, listIndexes...) {
		if !relationExists(ctx, t, db, name) {
			t.Fatalf("%s missing after down to 2", name)
		}
	}

	// The next step down removes only 00002's indexes.
	if _, err := p.DownTo(ctx, 1); err != nil {
		t.Fatalf("down to 1: %v", err)
	}
	for _, name := range listIndexes {
		if relationExists(ctx, t, db, name) {
			t.Fatalf("index %s still exists after down to 1", name)
		}
	}
	for _, name := range tables {
		if !relationExists(ctx, t, db, name) {
			t.Fatalf("table %s missing after down to 1", name)
		}
	}

	if _, err := p.DownTo(ctx, 0); err != nil {
		t.Fatalf("down: %v", err)
	}
	if v, err := p.GetDBVersion(ctx); err != nil || v != 0 {
		t.Fatalf("after down: version %d, err %v; want 0", v, err)
	}
	for _, name := range tables {
		if relationExists(ctx, t, db, name) {
			t.Fatalf("table %s still exists after down", name)
		}
	}

	up("second up")
}

// relationExists reports whether a table or index called name exists.
func relationExists(ctx context.Context, t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	err := db.QueryRowContext(ctx, `SELECT to_regclass('public.' || $1) IS NOT NULL`, name).Scan(&exists)
	if err != nil {
		t.Fatalf("check relation %s: %v", name, err)
	}
	return exists
}
