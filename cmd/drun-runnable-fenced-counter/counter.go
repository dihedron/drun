package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dihedron/drun/pkg/metadata"
	"github.com/dihedron/drun/pkg/runnable"
	"github.com/jackc/pgx/v5"
)

// schema is created by the first incarnation; the fence is §8.5's
const schema = `
CREATE TABLE IF NOT EXISTS drun_fence (runnable text PRIMARY KEY, epoch bigint NOT NULL);
CREATE TABLE IF NOT EXISTS counter_rows (
	id bigserial PRIMARY KEY,
	runnable text NOT NULL,
	epoch bigint NOT NULL,
	seq bigint NOT NULL,
	at timestamptz NOT NULL DEFAULT now()
);`

type config struct {
	DSNFile  string `json:"dsn_file"`
	Interval string `json:"interval"`
}

type settings struct {
	dsnFile  string
	interval time.Duration
}

func parse(raw []byte) (settings, error) {
	var c config
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return settings{}, fmt.Errorf("configuration: %w", err)
	}
	s := settings{dsnFile: c.DSNFile, interval: time.Second}
	if !filepath.IsAbs(c.DSNFile) {
		return settings{}, fmt.Errorf("dsn_file %q is not an absolute path", c.DSNFile)
	}
	if c.Interval != "" {
		d, err := time.ParseDuration(c.Interval)
		if err != nil {
			return settings{}, fmt.Errorf("interval: %w", err)
		}
		if d <= 0 {
			return settings{}, errors.New("interval: must be positive")
		}
		s.interval = d
	}
	return s, nil
}

// readDSN reads the connection string; it is never logged
func readDSN(path string) (string, error) {
	b, err := os.ReadFile(path) //nolint:gosec // the configured secret file
	if err != nil {
		return "", fmt.Errorf("dsn_file: %w", err)
	}
	dsn := strings.TrimSpace(string(b))
	if dsn == "" {
		return "", errors.New("dsn_file is empty")
	}
	return dsn, nil
}

type counter struct{}

func (counter) Describe() runnable.Description {
	return runnable.Description{Name: "fenced-counter", Version: metadata.Version, Kind: runnable.Service, Stateful: true}
}

func (counter) Validate(_ context.Context, raw []byte) error {
	s, err := parse(raw)
	if err != nil {
		return err
	}
	_, err = readDSN(s.dsnFile)
	return err
}

func (counter) Run(ctx context.Context, inc runnable.Incarnation, host runnable.Host) error {
	s, err := parse(inc.Config)
	if err != nil {
		return err
	}
	dsn, err := readDSN(s.dsnFile)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return stopped(ctx, fmt.Errorf("connecting: %w", err))
	}
	defer conn.Close(context.Background()) //nolint:errcheck // the incarnation is over
	if _, err := conn.Exec(ctx, schema); err != nil {
		return stopped(ctx, fmt.Errorf("schema: %w", err))
	}
	if err := claim(ctx, conn, inc.Runnable, inc.Epoch); err != nil {
		return stopped(ctx, err)
	}

	var seq uint64
	if cp := inc.Checkpoint; cp != nil && len(cp.Data) == 8 {
		seq = binary.BigEndian.Uint64(cp.Data)
	}
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		seq++
		if err := write(ctx, conn, inc.Runnable, inc.Epoch, seq); err != nil {
			return stopped(ctx, err)
		}
		if _, err := host.Checkpoint(ctx, binary.BigEndian.AppendUint64(nil, seq)); err != nil {
			return stopped(ctx, err)
		}
		if err := host.Status(ctx, true, fmt.Sprintf("seq=%d", seq)); err != nil {
			return stopped(ctx, err)
		}
	}
}

// stopped: an error after the incarnation was told to stop is the stop
func stopped(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// claim takes the fence for epoch; a newer epoch holding it means this
// incarnation is stale (§8.5). The row lock waits for the predecessor's
// open transactions.
func claim(ctx context.Context, conn *pgx.Conn, name string, epoch uint64) error {
	tag, err := conn.Exec(ctx, `INSERT INTO drun_fence VALUES ($1, $2)
		ON CONFLICT (runnable) DO UPDATE SET epoch = EXCLUDED.epoch
		WHERE drun_fence.epoch < EXCLUDED.epoch`, name, int64(epoch)) //nolint:gosec // epochs fit
	if err != nil {
		return fmt.Errorf("claiming the fence: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return runnable.ErrFenced
	}
	return nil
}

// write appends a row in a transaction that holds the fence at epoch
func write(ctx context.Context, conn *pgx.Conn, name string, epoch, seq uint64) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // a no-op after the commit
	var fence int64
	if err := tx.QueryRow(ctx, `SELECT epoch FROM drun_fence WHERE runnable = $1 FOR SHARE`, name).Scan(&fence); err != nil {
		return fmt.Errorf("reading the fence: %w", err)
	}
	if fence != int64(epoch) { //nolint:gosec // epochs fit
		return runnable.ErrFenced
	}
	if _, err := tx.Exec(ctx, `INSERT INTO counter_rows (runnable, epoch, seq) VALUES ($1, $2, $3)`, name, int64(epoch), int64(seq)); err != nil { //nolint:gosec // they fit
		return fmt.Errorf("insert: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
