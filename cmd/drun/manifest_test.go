package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dihedron/drun/internal/config"
	"github.com/dihedron/drun/pkg/runnable"
	"github.com/dihedron/drun/pkg/runnable/manifest"
)

// §8.8: a runnable with a key is pinned to its verified manifest's digest;
// one whose manifest fails is left unpinned, with the reason, and the
// loader refuses it (FR-16)
func TestResolveManifests(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keys := t.TempDir()
	if err := os.WriteFile(filepath.Join(keys, "acme.pub"), []byte(base64.StdEncoding.EncodeToString(pub)+"\n"), 0o644); err != nil { //nolint:gosec // public
		t.Fatal(err)
	}
	dir := t.TempDir()
	sum := strings.Repeat("cd", 32)
	for _, name := range []string{"good", "old"} {
		bin := filepath.Join(dir, name)
		if err := os.WriteFile(bin, []byte("#!/bin/true\n"), 0o755); err != nil { //nolint:gosec // a test binary
			t.Fatal(err)
		}
		drun := map[string]string{"good": "[1.0.0,2.0.0)", "old": "[0.1.0]"}[name]
		if err := manifest.Write(bin, "", priv, manifest.Manifest{Name: name, Version: "1.0.0", Drun: drun, SHA256: sum, Protocol: runnable.Handshake.ProtocolVersion}); err != nil {
			t.Fatal(err)
		}
	}
	pinned := strings.Repeat("0", 64)
	cfg := &config.Config{Runnables: []config.Runnable{
		{Name: "good", Binary: filepath.Join(dir, "good"), Key: "acme"},
		{Name: "old", Binary: filepath.Join(dir, "old"), Key: "acme"},
		{Name: "stranger", Binary: filepath.Join(dir, "good"), Key: "nobody"},
		{Name: "pinned", Binary: "/x", SHA256: pinned},
	}}
	failed := resolveManifests(cfg, keys, "1.4.0", false, slog.New(slog.DiscardHandler))
	if cfg.Runnables[0].SHA256 != sum || cfg.Runnables[3].SHA256 != pinned {
		t.Fatalf("pinned: %+v", cfg.Runnables)
	}
	if len(failed) != 2 || !strings.Contains(failed["old"].Error(), "version") || !strings.Contains(failed["stranger"].Error(), "nobody") {
		t.Fatalf("failed: %v", failed)
	}
	if cfg.Runnables[1].SHA256 != "" {
		t.Fatal("a failed manifest pinned its runnable")
	}
}
