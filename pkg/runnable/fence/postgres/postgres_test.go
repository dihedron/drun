package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dihedron/drun/pkg/runnable"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testDB returns a pool on a database of this test run's own, on the
// server of DRUN_TEST_POSTGRES (set by make test-postgres): other packages'
// tests share that server, and these drop the fence table.
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DRUN_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("DRUN_TEST_POSTGRES is not set: run make test-postgres")
	}
	admin, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background()) //nolint:errcheck // test
	name := fmt.Sprintf("fence_test_%d_%d", os.Getpid(), time.Now().UnixNano())
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if c, err := pgx.Connect(context.Background(), dsn); err == nil {
			_, _ = c.Exec(context.Background(), "DROP DATABASE "+name)
			_ = c.Close(context.Background())
		}
	})
	return pool
}

func inc(cluster string, epoch uint64) runnable.Incarnation {
	return runnable.Incarnation{Cluster: cluster, Runnable: "svc", Epoch: epoch}
}

// insert writes one row of epoch in a fenced transaction
func insert(ctx context.Context, f *Fence, db DB, epoch uint64) error {
	return f.Tx(ctx, db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO rows (epoch) VALUES ($1)", int64(epoch)) //nolint:gosec // test epochs fit
		return err
	})
}

func rows(t *testing.T, db DB) []int64 {
	t.Helper()
	r, err := db.(*pgxpool.Pool).Query(t.Context(), "SELECT epoch FROM rows ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(r, pgx.RowTo[int64])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func withRows(t *testing.T) *pgxpool.Pool {
	t.Helper()
	db := testDB(t)
	if _, err := db.Exec(t.Context(), "CREATE TABLE rows (id bigserial PRIMARY KEY, epoch bigint NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestClaimAndFence(t *testing.T) {
	db := withRows(t)
	ctx := t.Context()
	old, err := Claim(ctx, db, inc("c", 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := insert(ctx, old, db, 1); err != nil {
		t.Fatal(err)
	}
	// the same epoch claims again (a reconnect)
	if _, err := Claim(ctx, db, inc("c", 1)); err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	newer, err := Claim(ctx, db, inc("c", 2))
	if err != nil {
		t.Fatal(err)
	}
	if err := insert(ctx, old, db, 1); !errors.Is(err, runnable.ErrFenced) {
		t.Fatalf("a stale write: %v", err)
	}
	if _, err := Claim(ctx, db, inc("c", 1)); !errors.Is(err, runnable.ErrFenced) {
		t.Fatalf("a stale claim: %v", err)
	}
	if err := insert(ctx, newer, db, 2); err != nil {
		t.Fatal(err)
	}
	// another cluster's runnable of the same name has a fence of its own
	other, err := Claim(ctx, db, inc("d", 1))
	if err != nil {
		t.Fatalf("another cluster: %v", err)
	}
	if err := insert(ctx, other, db, 1); err != nil {
		t.Fatal(err)
	}
	if got := rows(t, db); fmt.Sprint(got) != "[1 2 1]" {
		t.Fatalf("rows: %v", got)
	}
}

// the claim waits for the predecessor's open transaction, and the
// predecessor's next one is refused (§8.5)
func TestClaimWaitsForOpenTransaction(t *testing.T) {
	db := withRows(t)
	ctx := t.Context()
	old, err := Claim(ctx, db, inc("c", 1))
	if err != nil {
		t.Fatal(err)
	}
	inside, release := make(chan struct{}), make(chan struct{})
	oldDone := make(chan error, 1)
	go func() {
		oldDone <- old.Tx(ctx, db, func(tx pgx.Tx) error {
			close(inside)
			<-release
			_, err := tx.Exec(ctx, "INSERT INTO rows (epoch) VALUES (1)")
			return err
		})
	}()
	<-inside
	claimed := make(chan error, 1)
	go func() { _, err := Claim(ctx, db, inc("c", 2)); claimed <- err }()
	select {
	case err := <-claimed:
		t.Fatalf("the claim did not wait for the open transaction: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	if err := <-oldDone; err != nil {
		t.Fatalf("the open transaction: %v", err)
	}
	if err := <-claimed; err != nil {
		t.Fatalf("the claim: %v", err)
	}
	if err := insert(ctx, old, db, 1); !errors.Is(err, runnable.ErrFenced) {
		t.Fatalf("after the claim: %v", err)
	}
}

func TestCheckInCallersTransaction(t *testing.T) {
	db := withRows(t)
	ctx := t.Context()
	old, err := Claim(ctx, db, inc("c", 1))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // test
	if err := old.Check(ctx, tx); err != nil {
		t.Fatalf("check: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := Claim(ctx, db, inc("c", 2)); err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // test
	if err := old.Check(ctx, tx); !errors.Is(err, runnable.ErrFenced) {
		t.Fatalf("a stale check: %v", err)
	}
}

func TestTxRollsBack(t *testing.T) {
	db := withRows(t)
	ctx := t.Context()
	f, err := Claim(ctx, db, inc("c", 1))
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	err = f.Tx(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO rows (epoch) VALUES (1)"); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("tx: %v", err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Fatalf("rolled back rows: %v", got)
	}
	if err := f.Tx(ctx, db, func(pgx.Tx) error { return runnable.ErrFenced }); err != runnable.ErrFenced { //nolint:errorlint // unwrapped, as specified
		t.Fatalf("ErrFenced from fn: %v", err)
	}
}

func TestEpochOutOfRange(t *testing.T) {
	db := testDB(t)
	if _, err := Claim(t.Context(), db, inc("c", math.MaxInt64+1)); err == nil || errors.Is(err, runnable.ErrFenced) {
		t.Fatalf("an epoch above MaxInt64: %v", err)
	}
}

// first-ever claims racing on CREATE TABLE IF NOT EXISTS all succeed
func TestConcurrentFirstClaims(t *testing.T) {
	db := testDB(t)
	for round := range 5 {
		if _, err := db.Exec(t.Context(), "DROP TABLE IF EXISTS drun_fence"); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for i := range 8 {
			wg.Go(func() {
				if _, err := Claim(t.Context(), db, inc(fmt.Sprintf("c%d-%d", round, i), 1)); err != nil {
					errs <- err
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("round %d: %v", round, err)
		}
	}
}
