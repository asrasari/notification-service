// Package testdb gives integration tests a fresh, migrated Postgres database
// each (D-017).
//
// Each test package starts one Postgres container in TestMain:
//
//	func TestMain(m *testing.M) { testdb.Main(m) }
//
// Main applies the migrations once into a template database. New then clones
// it for every test with CREATE DATABASE … TEMPLATE, which takes milliseconds,
// so tests are isolated and can run in parallel.
//
// With -short, Main starts no container and New skips the test. KEEP_TEST_DB=1
// keeps the container and the test databases after the run and logs a psql
// command for each one.
package testdb

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver for goose
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/asrasari/notification-service/migrations"
)

// Image is the Postgres image the tests run against. It must match the
// postgres service in docker-compose.yml; TestImageMatchesCompose checks this.
const Image = "postgres:17-alpine"

const (
	user         = "test"
	password     = "test"
	adminDB      = "postgres"
	templateName = "notifications_template"
	keepEnv      = "KEEP_TEST_DB"
)

// state is set by Main and read by New.
var (
	ready   bool
	keep    bool
	baseURL *url.URL      // container URL without a database name
	admin   *pgxpool.Pool // connection to adminDB, used for CREATE/DROP DATABASE
	// createMu serialises CREATE DATABASE: Postgres refuses to copy a template
	// that another session is using, and a concurrent clone may count as one.
	createMu sync.Mutex
	counter  atomic.Int64
)

// Main starts the Postgres container, builds the migrated template database,
// runs the package's tests and removes the container afterwards. Call it from
// TestMain. It exits the process with the tests' result.
func Main(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	flag.Parse()
	if testing.Short() {
		return m.Run()
	}

	keep = os.Getenv(keepEnv) == "1"
	if keep {
		// Ryuk, testcontainers' reaper, would remove the container when the
		// test binary exits. It reads this variable on first use, which is below.
		if err := os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true"); err != nil {
			fmt.Fprintf(os.Stderr, "testdb: %v\n", err)
			return 1
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ctr, err := tcpostgres.Run(ctx, Image,
		tcpostgres.WithUsername(user),
		tcpostgres.WithPassword(password),
		tcpostgres.WithDatabase(adminDB),
		tcpostgres.BasicWaitStrategies(),
	)
	if ctr != nil && !keep {
		defer func() {
			if err := testcontainers.TerminateContainer(ctr); err != nil {
				fmt.Fprintf(os.Stderr, "testdb: terminate container: %v\n", err)
			}
		}()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "testdb: start Postgres container (is Docker running? use -short to skip): %v\n", err)
		return 1
	}

	if err := setup(ctx, ctr); err != nil {
		fmt.Fprintf(os.Stderr, "testdb: %v\n", err)
		return 1
	}
	defer admin.Close()

	ready = true
	code := m.Run()

	if keep {
		fmt.Fprintf(os.Stderr, "testdb: %s=1, container %s kept; remove it with: docker rm -f %s\n",
			keepEnv, ctr.GetContainerID()[:12], ctr.GetContainerID()[:12])
	}
	return code
}

// setup connects to the container and migrates the template database.
func setup(ctx context.Context, ctr *tcpostgres.PostgresContainer) error {
	host, err := ctr.Host(ctx)
	if err != nil {
		return fmt.Errorf("container host: %w", err)
	}
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return fmt.Errorf("container port: %w", err)
	}
	baseURL = &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, password),
		Host:     net.JoinHostPort(host, port.Port()),
		RawQuery: "sslmode=disable",
	}

	admin, err = pgxpool.New(ctx, dsn(adminDB))
	if err != nil {
		return fmt.Errorf("connect admin pool: %w", err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+ident(templateName)); err != nil {
		admin.Close()
		return fmt.Errorf("create template database: %w", err)
	}
	if err := migrate(ctx, dsn(templateName)); err != nil {
		admin.Close()
		return fmt.Errorf("migrate template database: %w", err)
	}
	return nil
}

// migrate applies every migration and closes its connections, because
// Postgres refuses to clone a template that has open sessions.
func migrate(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }() // only the template connection; nothing to do on error
	p, err := migrations.NewProvider(db)
	if err != nil {
		return err
	}
	_, err = p.Up(ctx)
	return err
}

// New creates a database for this test, cloned from the migrated template,
// and returns its connection string. The database is dropped when the test
// ends, unless KEEP_TEST_DB=1. With -short, New skips the test.
func New(t testing.TB) string {
	t.Helper()
	if testing.Short() {
		t.Skip("needs Docker (Postgres container); skipped with -short")
	}
	if !ready {
		t.Fatal("testdb: not started; add `func TestMain(m *testing.M) { testdb.Main(m) }` to this package")
	}

	name := dbName(t.Name())
	ctx := context.Background()

	createMu.Lock()
	_, err := admin.Exec(ctx, "CREATE DATABASE "+ident(name)+" TEMPLATE "+ident(templateName))
	createMu.Unlock()
	if err != nil {
		t.Fatalf("testdb: create database %s: %v", name, err)
	}

	connStr := dsn(name)
	t.Cleanup(func() {
		if keep {
			t.Logf("testdb: database kept: psql '%s'", connStr)
			return
		}
		// FORCE closes connections the test left open (Postgres 13+).
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+ident(name)+" WITH (FORCE)"); err != nil {
			t.Errorf("testdb: drop database %s: %v", name, err)
		}
	})
	return connStr
}

func dsn(dbname string) string {
	u := *baseURL
	u.Path = "/" + dbname
	return u.String()
}

func ident(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

var unsafeChars = regexp.MustCompile(`[^a-z0-9_]+`)

// dbName builds a unique, readable database name from the test name, so a
// kept database can be matched to its test. Postgres limits names to 63 bytes.
func dbName(testName string) string {
	n := counter.Add(1)
	base := unsafeChars.ReplaceAllString(strings.ToLower(testName), "_")
	base = strings.Trim(base, "_")
	if len(base) > 45 {
		base = base[:45]
	}
	return fmt.Sprintf("t%d_%s", n, base)
}
