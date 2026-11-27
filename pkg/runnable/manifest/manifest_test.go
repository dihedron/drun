package manifest

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dihedron/drun/pkg/runnable"
)

var protocol = runnable.Handshake.ProtocolVersion

// installed lays out a directory as a runnable package does: the binary,
// its manifest and signature, signed by key
func installed(t *testing.T, key ed25519.PrivateKey, m Manifest) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "drun-runnable-"+m.Name)
	if err := os.WriteFile(bin, []byte("#!/bin/true\n"), 0o755); err != nil { //nolint:gosec // a test binary
		t.Fatal(err)
	}
	if err := Write(bin, "", key, m); err != nil {
		t.Fatal(err)
	}
	return bin
}

func newKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

// §8.8: each verification step refuses with its name
func TestVerify(t *testing.T) {
	pub, priv := newKey(t)
	other, otherPriv := newKey(t)
	sum := strings.Repeat("ab", 32)
	good := Manifest{Name: "ticker", Version: "0.9.0", Drun: "[1.2.3]", SHA256: sum, Protocol: protocol}

	bin := installed(t, priv, good)
	m, err := Verify(bin, "ticker", "1.2.3", []ed25519.PublicKey{other, pub}, false)
	if err != nil || m.SHA256 != sum || m.Version != "0.9.0" {
		t.Fatalf("a valid manifest: %+v %v", m, err)
	}

	for _, c := range []struct {
		name, want string
		bin        func() string
		version    string
		keys       []ed25519.PublicKey
	}{
		{"unknown key", "signature", func() string { return installed(t, otherPriv, good) }, "1.2.3", []ed25519.PublicKey{pub}},
		{"no key", "signature", func() string { return bin }, "1.2.3", nil},
		{"another version", "version", func() string { return bin }, "1.2.4", []ed25519.PublicKey{pub}},
		{"a snapshot of drun", "version", func() string { return bin }, "1.2.3-SNAPSHOT-abc", []ed25519.PublicKey{pub}},
		{"drun's version malformed", "version", func() string { return bin }, "dev", []ed25519.PublicKey{pub}},
		{"outside the range", "version", func() string {
			return installed(t, priv, Manifest{Name: "ticker", Version: "0.9.0", Drun: "[1.0.0,1.2.3)", SHA256: sum, Protocol: protocol})
		}, "1.2.3", []ed25519.PublicKey{pub}},
		{"malformed range", "version", func() string {
			return installed(t, priv, Manifest{Name: "ticker", Version: "0.9.0", Drun: "1.2.3", SHA256: sum, Protocol: protocol})
		}, "1.2.3", []ed25519.PublicKey{pub}},
		{"another name", "name", func() string {
			return installed(t, priv, Manifest{Name: "other", Version: "0.9.0", Drun: "[1.2.3]", SHA256: sum, Protocol: protocol})
		}, "1.2.3", []ed25519.PublicKey{pub}},
		{"another protocol", "protocol", func() string {
			return installed(t, priv, Manifest{Name: "ticker", Version: "0.9.0", Drun: "[1.2.3]", SHA256: sum, Protocol: protocol + 1})
		}, "1.2.3", []ed25519.PublicKey{pub}},
		{"a malformed digest", "sha256", func() string {
			return installed(t, priv, Manifest{Name: "ticker", Version: "0.9.0", Drun: "[1.2.3]", SHA256: "abc", Protocol: protocol})
		}, "1.2.3", []ed25519.PublicKey{pub}},
		{"tampered manifest", "signature", func() string {
			b := installed(t, priv, good)
			data, _ := os.ReadFile(b + ".manifest")
			_ = os.WriteFile(b+".manifest", []byte(strings.Replace(string(data), "1.2.3", "1.2.4", 1)), 0o644) //nolint:gosec // test
			return b
		}, "1.2.4", []ed25519.PublicKey{pub}},
		{"no manifest", "manifest", func() string {
			b := installed(t, priv, good)
			_ = os.Remove(b + ".manifest")
			return b
		}, "1.2.3", []ed25519.PublicKey{pub}},
	} {
		if _, err := Verify(c.bin(), "ticker", c.version, c.keys, false); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error about the %s", c.name, err, c.want)
		}
	}

	// a range of versions
	ranged := installed(t, priv, Manifest{Name: "ticker", Version: "0.9.0", Drun: "[1.0.0,2.0.0)", SHA256: sum, Protocol: protocol})
	for _, v := range []string{"1.0.0", "1.2.3", "1.99.0"} {
		if _, err := Verify(ranged, "ticker", v, []ed25519.PublicKey{pub}, false); err != nil {
			t.Errorf("drun %s in [1.0.0,2.0.0): %v", v, err)
		}
	}
	if os.Getuid() != 0 {
		if _, err := Verify(bin, "ticker", "1.2.3", []ed25519.PublicKey{pub}, true); err == nil || !strings.Contains(err.Error(), "root") {
			t.Errorf("a user's directory: %v, want refused", err)
		}
	}
}

func TestLoadKeys(t *testing.T) {
	pub, _ := newKey(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "acme.pub"), []byte("# acme\n"+base64.StdEncoding.EncodeToString(pub)+"\n"), 0o644); err != nil { //nolint:gosec // public
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "empty.pub"), []byte("# no key\n"), 0o644); err != nil { //nolint:gosec // public
		t.Fatal(err)
	}
	keys, err := LoadKeys(dir, "acme", false)
	if err != nil || len(keys) != 1 || !keys[0].Equal(pub) {
		t.Fatalf("acme: %v %v", keys, err)
	}
	for _, name := range []string{"nothere", "empty"} {
		if _, err := LoadKeys(dir, name, false); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if os.Getuid() != 0 {
		if _, err := LoadKeys(dir, "acme", true); err == nil || !strings.Contains(err.Error(), "root") {
			t.Errorf("a user's key directory: %v, want refused", err)
		}
	}
}

// the release's key file, which the drun package installs as drun.pub
func TestReleaseKey(t *testing.T) {
	keys, err := LoadKeys("../../../packaging", "runnable-signing", false)
	if err != nil || len(keys) == 0 {
		t.Fatalf("packaging/runnable-signing.pub: %v %v", keys, err)
	}
}

func TestParseKeys(t *testing.T) {
	pub, _ := newKey(t)
	keys, err := ParseKeys([]byte("# drun runnable signing keys\n\n" + base64.StdEncoding.EncodeToString(pub) + "\n"))
	if err != nil || len(keys) != 1 || !keys[0].Equal(pub) {
		t.Fatalf("%v %v", keys, err)
	}
	for _, bad := range []string{"not base64!\n", base64.StdEncoding.EncodeToString([]byte("short")) + "\n"} {
		if _, err := ParseKeys([]byte(bad)); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
