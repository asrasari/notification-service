package testdb

import (
	"context"
	"os"
	"regexp"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
)

func TestMain(m *testing.M) { Main(m) }

// TestImageMatchesCompose keeps Image and the docker-compose postgres image in
// sync (D-017: tests run on the same Postgres as the stack). It needs no Docker.
func TestImageMatchesCompose(t *testing.T) {
	compose, err := os.ReadFile("../../docker-compose.yml")
	if err != nil {
		t.Fatalf("read docker-compose.yml: %v", err)
	}
	re := regexp.MustCompile(`(?m)^\s*image:\s*["']?(postgres:[^\s"']+)`)
	matches := re.FindAllSubmatch(compose, -1)
	if len(matches) == 0 {
		t.Fatal("no postgres image found in docker-compose.yml")
	}
	for _, m := range matches {
		if got := string(m[1]); got != Image {
			t.Errorf("docker-compose.yml uses %s, testdb.Image is %s; keep them equal", got, Image)
		}
	}
}

func TestDBName(t *testing.T) {
	long := "TestSomething/with a very long subtest name that goes past the Postgres limit"
	got := dbName(long)
	if len(got) > 63 {
		t.Errorf("dbName length %d > 63: %q", len(got), got)
	}
	if !regexp.MustCompile(`^t\d+_[a-z0-9_]+$`).MatchString(got) {
		t.Errorf("dbName %q has unexpected characters", got)
	}
	if a, b := dbName("TestX"), dbName("TestX"); a == b {
		t.Errorf("dbName not unique: %q twice", a)
	}
}

// TestNewIsMigratedAndIsolated checks that each database is a migrated clone
// and that writes in one are invisible in another.
func TestNewIsMigratedAndIsolated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	connect := func(dsn string) *pgx.Conn {
		t.Helper()
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		return c
	}
	a, b := connect(New(t)), connect(New(t))

	rows, err := a.Query(ctx, `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'goose_db_version' ORDER BY 1`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("scan tables: %v", err)
	}
	if diff := cmp.Diff([]string{"delivery_attempts", "notifications", "worker_heartbeats"}, tables); diff != "" {
		t.Errorf("tables (-want +got):\n%s", diff)
	}

	if _, err := a.Exec(ctx, `CREATE TABLE only_in_a (id int)`); err != nil {
		t.Fatalf("create table in a: %v", err)
	}
	var exists bool
	if err := b.QueryRow(ctx, `SELECT to_regclass('public.only_in_a') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatalf("check b: %v", err)
	}
	if exists {
		t.Error("table created in one test database is visible in another")
	}
}

// TestNewParallel clones many databases at once, as parallel tests will.
func TestNewParallel(t *testing.T) {
	for i := range 8 {
		t.Run("clone", func(t *testing.T) {
			t.Parallel()
			c, err := pgx.Connect(context.Background(), New(t))
			if err != nil {
				t.Fatalf("clone %d: connect: %v", i, err)
			}
			_ = c.Close(context.Background())
		})
	}
}
