package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dihedron/drun/pkg/runnable"
	"github.com/dihedron/drun/pkg/runnable/conformance"
	"github.com/jackc/pgx/v5"
)

func dsnFile(t *testing.T, dsn string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(path, []byte(dsn+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestValidate(t *testing.T) {
	ok := dsnFile(t, "postgres://localhost/x")
	empty := dsnFile(t, " ")
	for cfg, valid := range map[string]bool{
		`{"dsn_file":"` + ok + `"}`:                     true,
		`{"dsn_file":"` + ok + `","interval":"100ms"}`:  true,
		`{"dsn_file":"` + ok + `","interval":"-1s"}`:    false,
		`{"dsn_file":"relative"}`:                       false,
		`{"dsn_file":"/nonexistent/dsn"}`:               false,
		`{"dsn_file":"` + empty + `"}`:                  false,
		`{"dsn_file":"` + ok + `","password":"secret"}`: false,
	} {
		if err := (counter{}).Validate(t.Context(), []byte(cfg)); (err == nil) != valid {
			t.Errorf("%s: %v", cfg, err)
		}
	}
}

// postgres returns a connection to the test database (DRUN_TEST_POSTGRES,
// set by make test-postgres), with the example's tables emptied
func postgres(t *testing.T) (string, *pgx.Conn) {
	t.Helper()
	dsn := os.Getenv("DRUN_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("DRUN_TEST_POSTGRES is not set: run make test-postgres")
	}
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(t.Context(), schema+"TRUNCATE drun_fence, counter_rows;"); err != nil {
		t.Fatal(err)
	}
	return dsn, conn
}

func TestConformance(t *testing.T) {
	dsn, conn := postgres(t)
	cfg := []byte(`{"dsn_file":"` + dsnFile(t, dsn) + `","interval":"20ms"}`)
	conformance.Check(t, func(t *testing.T) runnable.Instance {
		// every check starts at epoch 1: a fresh fence each time
		if _, err := conn.Exec(t.Context(), "TRUNCATE drun_fence"); err != nil {
			t.Fatal(err)
		}
		return runnable.InProcess(counter{})
	}, conformance.Options{Config: cfg})
}

// §8.5: once a newer incarnation claims the fence, the older one's next
// write is refused and it returns ErrFenced; the checker finds no violation
func TestFencing(t *testing.T) {
	dsn, _ := postgres(t)
	cfg := []byte(`{"dsn_file":"` + dsnFile(t, dsn) + `","interval":"10ms"}`)
	run := func(ctx context.Context, epoch uint64) chan error {
		done := make(chan error, 1)
		go func() {
			done <- counter{}.Run(ctx, runnable.Incarnation{Runnable: "fenced-counter", Epoch: epoch, Config: cfg}, conformance.NewHost())
		}()
		return done
	}
	old := run(t.Context(), 1)
	time.Sleep(200 * time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	newer := run(ctx, 2)
	select {
	case err := <-old:
		if !errors.Is(err, runnable.ErrFenced) {
			t.Fatalf("the stale incarnation returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stale incarnation kept running")
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-newer; err != nil {
		t.Fatalf("the current incarnation: %v", err)
	}
	// a stale incarnation cannot even start
	if err := (counter{}).Run(t.Context(), runnable.Incarnation{Runnable: "fenced-counter", Epoch: 1, Config: cfg}, conformance.NewHost()); !errors.Is(err, runnable.ErrFenced) {
		t.Fatalf("a stale start: %v", err)
	}
	var out bytes.Buffer
	if code := check([]string{"--dsn-file", dsnFile(t, dsn)}, &out, &out); code != 0 || !strings.Contains(out.String(), "2 epochs") {
		t.Fatalf("check: %d %s", code, out.String())
	}
}

func TestCheckerFindsViolations(t *testing.T) {
	dsn, conn := postgres(t)
	if _, err := conn.Exec(t.Context(), `INSERT INTO counter_rows (runnable, epoch, seq) VALUES
		('fenced-counter', 1, 1), ('fenced-counter', 2, 2), ('fenced-counter', 2, 2), ('fenced-counter', 1, 3)`); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	code := check([]string{"--dsn-file", dsnFile(t, dsn)}, &out, &out)
	if code != 1 || !strings.Contains(out.String(), "epoch 1 committed after epoch 2") || !strings.Contains(out.String(), "1 rows repeated") {
		t.Fatalf("check: %d %s", code, out.String())
	}
	if code := check([]string{"--dsn-file", "/nonexistent"}, &out, &out); code != 2 {
		t.Fatalf("check without a DSN: %d", code)
	}
}
