// Package manifest vouches for a runnable binary through a manifest signed
// by its publisher (drun_specification.md §8.8, FW-7): the node trusts the
// manifest's SHA-256 as it trusts a configured one. Standard library only.
// The design, and much of the code, is veho's pkg/plugin/process manifest.
package manifest

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/dihedron/drun/pkg/runnable"
	"github.com/dihedron/drun/pkg/semver"
)

// KeyDir holds the publishers' public keys, <name>.pub, the release's as
// drun.pub.
const KeyDir = "/etc/drun/runnable-keys"

// Manifest vouches for one runnable binary.
type Manifest struct {
	Name     string `json:"name"`
	Version  string `json:"version"` // the runnable's own
	Drun     string `json:"drun"`    // the versions of drun it supports, a semver range
	SHA256   string `json:"sha256"`
	Protocol uint   `json:"protocol"`
}

// Write writes m and its Ed25519 signature as <binary>.manifest and
// <binary>.manifest.sig, in dir if not empty, else next to binary.
func Write(binary, dir string, key ed25519.PrivateKey, m Manifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("runnable %s: manifest: %w", m.Name, err)
	}
	data = append(data, '\n')
	if dir == "" {
		dir = filepath.Dir(binary)
	}
	base := filepath.Join(dir, filepath.Base(binary)+".manifest")
	if err := os.WriteFile(base, data, 0o644); err != nil { //nolint:gosec // public, as the binary
		return fmt.Errorf("runnable %s: manifest: %w", m.Name, err)
	}
	sig := base64.StdEncoding.EncodeToString(ed25519.Sign(key, data)) + "\n"
	if err := os.WriteFile(base+".sig", []byte(sig), 0o644); err != nil { //nolint:gosec // public
		return fmt.Errorf("runnable %s: manifest: %w", m.Name, err)
	}
	return nil
}

// ParseKeys reads public keys, one base64 Ed25519 key a line; blank lines
// and lines starting with # are skipped.
func ParseKeys(data []byte) ([]ed25519.PublicKey, error) {
	var keys []ed25519.PublicKey
	sc := bufio.NewScanner(bytes.NewReader(data))
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, err := base64.StdEncoding.DecodeString(line)
		if err != nil || len(k) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("runnable keys: line %d is not a base64 Ed25519 public key", n)
		}
		keys = append(keys, ed25519.PublicKey(k))
	}
	return keys, sc.Err()
}

// LoadKeys reads the keys of dir/name.pub, one at least; with
// requireRootOwned, the directory and the file must be root's and not
// writable by group or others.
func LoadKeys(dir, name string, requireRootOwned bool) ([]ed25519.PublicKey, error) {
	path := filepath.Join(dir, name+".pub")
	if requireRootOwned {
		for _, p := range []string{dir, path} {
			if err := rootOwned(p); err != nil {
				return nil, fmt.Errorf("runnable key %s: %w", name, err)
			}
		}
	}
	data, err := os.ReadFile(path) //nolint:gosec // the key directory
	if err != nil {
		return nil, fmt.Errorf("runnable key %s: %w", name, err)
	}
	keys, err := ParseKeys(data)
	if err != nil {
		return nil, fmt.Errorf("runnable key %s: %w", name, err)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("runnable key %s: %s holds no key", name, path)
	}
	return keys, nil
}

// Verify checks the manifest of binary against keys, the configured name
// and drun's version, and returns it; its SHA256 then pins the binary.
func Verify(binary, name, version string, keys []ed25519.PublicKey, requireRootOwned bool) (Manifest, error) {
	fail := func(step string, err error) (Manifest, error) {
		return Manifest{}, fmt.Errorf("runnable %s: manifest %s: %w", name, step, err)
	}
	if requireRootOwned {
		for _, p := range []string{filepath.Dir(binary), binary + ".manifest", binary + ".manifest.sig"} {
			if err := rootOwned(p); err != nil {
				return fail("ownership", err)
			}
		}
	}
	data, err := os.ReadFile(binary + ".manifest") //nolint:gosec // the runnables' directory
	if err != nil {
		return fail("manifest", err)
	}
	sig, err := os.ReadFile(binary + ".manifest.sig") //nolint:gosec // the runnables' directory
	if err != nil {
		return fail("manifest", err)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(sig)))
	if err != nil || !slices.ContainsFunc(keys, func(k ed25519.PublicKey) bool { return ed25519.Verify(k, data, raw) }) {
		return fail("signature", errors.New("not signed by the runnable's key"))
	}
	var m Manifest
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return fail("manifest", err)
	}
	if m.Name != name {
		return fail("name", fmt.Errorf("the manifest is of %q", m.Name))
	}
	supported, err := semver.ParseRange(m.Drun)
	if err != nil {
		return fail("version", err)
	}
	v, err := semver.Parse(version)
	if err != nil {
		return fail("version", fmt.Errorf("drun's own: %w", err))
	}
	if !supported.Contains(v) {
		return fail("version", fmt.Errorf("%s %s supports drun %s, not %s: install one for this version", m.Name, m.Version, m.Drun, version))
	}
	if m.Protocol != runnable.Handshake.ProtocolVersion {
		return fail("protocol", fmt.Errorf("the runnable speaks protocol %d, drun %d", m.Protocol, runnable.Handshake.ProtocolVersion))
	}
	if sum, err := hex.DecodeString(m.SHA256); err != nil || len(sum) != 32 || strings.ToLower(m.SHA256) != m.SHA256 {
		return fail("sha256", fmt.Errorf("%q: want 64 lower-case hexadecimal digits", m.SHA256))
	}
	return m, nil
}

// rootOwned: root's, and only root can change it
func rootOwned(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != 0 {
		return fmt.Errorf("%s is not owned by root", path)
	}
	if fi.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%s is writable by group or others", path)
	}
	return nil
}
