// drun-runnable-ticker is the stateless example runnable: a service
// that counts, checkpointing the counter every interval, so that each
// incarnation resumes where the last acknowledged one stopped (AC-6).
//
// Its configuration, as JSON: {"interval": "1s"} (the default).
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dihedron/drun/pkg/metadata"
	"github.com/dihedron/drun/pkg/runnable"
)

func main() { runnable.Serve(ticker{}) }

type config struct {
	Interval string `json:"interval"`
}

// parse reads a configuration; unknown keys are refused
func parse(raw []byte) (time.Duration, error) {
	var c config
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return 0, fmt.Errorf("configuration: %w", err)
	}
	if c.Interval == "" {
		return time.Second, nil
	}
	d, err := time.ParseDuration(c.Interval)
	if err != nil {
		return 0, fmt.Errorf("interval: %w", err)
	}
	if d <= 0 {
		return 0, errors.New("interval: must be positive")
	}
	return d, nil
}

type ticker struct{}

func (ticker) Describe() runnable.Description {
	return runnable.Description{Name: "ticker", Version: metadata.Version, Kind: runnable.Service}
}

func (ticker) Validate(_ context.Context, raw []byte) error {
	_, err := parse(raw)
	return err
}

func (ticker) Run(ctx context.Context, inc runnable.Incarnation, host runnable.Host) error {
	interval, err := parse(inc.Config)
	if err != nil {
		return err
	}
	var n uint64
	if cp := inc.Checkpoint; cp != nil && len(cp.Data) == 8 {
		n = binary.BigEndian.Uint64(cp.Data)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		n++
		if _, err := host.Checkpoint(ctx, binary.BigEndian.AppendUint64(nil, n)); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err // ErrFenced included: Run must return
		}
		if err := host.Status(ctx, true, fmt.Sprintf("n=%d", n)); err != nil && ctx.Err() == nil {
			return err
		}
	}
}
