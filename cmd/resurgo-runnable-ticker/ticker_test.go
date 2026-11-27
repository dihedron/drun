package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dihedron/resurgo/pkg/runnable"
	"github.com/dihedron/resurgo/pkg/runnable/conformance"
)

func TestConformance(t *testing.T) {
	conformance.Check(t, func(*testing.T) runnable.Instance { return runnable.InProcess(ticker{}) },
		conformance.Options{Config: []byte(`{"interval":"20ms"}`)})
}

func TestValidate(t *testing.T) {
	for cfg, ok := range map[string]bool{`{}`: true, `null`: true, `{"interval":"1s"}`: true, `{"interval":"0s"}`: false, `{"interval":"soon"}`: false, `{"colour":"blue"}`: false} {
		if err := (ticker{}).Validate(t.Context(), []byte(cfg)); (err == nil) != ok {
			t.Errorf("%s: %v", cfg, err)
		}
	}
}

// it resumes from the checkpoint's counter
func TestResumes(t *testing.T) {
	h := conformance.NewHost()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	inc := runnable.Incarnation{Epoch: 2, Config: []byte(`{"interval":"10ms"}`),
		Checkpoint: &runnable.Checkpoint{Seq: 1, Data: binary.BigEndian.AppendUint64(nil, 41)}}
	_ = ticker{}.Run(ctx, inc, h)
	cps, _, _ := h.Snapshot()
	if len(cps) == 0 || binary.BigEndian.Uint64(cps[0]) != 42 {
		t.Fatalf("checkpoints %v", cps)
	}
}

// the binary itself, as the node loads it
func TestConformanceAsPlugin(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "ticker")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	data, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	conformance.Check(t, func(t *testing.T) runnable.Instance {
		inst, err := runnable.Load(t.Context(), runnable.Plugin{Path: bin, SHA256: hex.EncodeToString(sum[:]), Logger: slog.New(slog.DiscardHandler)})
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		return inst
	}, conformance.Options{Config: []byte(`{"interval":"20ms"}`)})
}
