package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const valid = `
node:
  id: node-a
  data_dir: /var/lib/resurgo
cluster:
  name: test
  bind: 0.0.0.0:7350
  advertise: 10.0.1.11:7350
  voters:
    - { id: node-a, address: 10.0.1.11:7350 }
    - { id: node-b, address: 10.0.1.12:7350 }
    - { id: node-c, address: 10.0.1.13:7350 }
  tls:
    cert: /etc/resurgo/tls/node.crt
    key: /etc/resurgo/tls/node.key
    ca: /etc/resurgo/tls/ca.crt
    peers:
      node-a: "spiffe://example.org/resurgo/node-a"
      node-b: "spiffe://example.org/resurgo/node-b"
      node-c: node-c.example.org
runnables:
  - name: svc
    binary: /usr/libexec/resurgo/runnables/svc
    sha256: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
`

func load(t *testing.T, text string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "resurgo.yaml")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

func mustLoad(t *testing.T, text string) *Config {
	t.Helper()
	cfg, err := load(t, text)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return cfg
}

func TestExampleLoads(t *testing.T) {
	data, err := os.ReadFile("../../examples/resurgo.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// the example's digest is a placeholder: put a well-formed one in
	text := strings.Replace(string(data), `"<the binary's SHA-256>"`, strings.Repeat("ab", 32), 1)
	cfg := mustLoad(t, text)
	if cfg.Node.ID != "node-a" || len(cfg.Cluster.Voters) != 3 || len(cfg.Runnables) != 1 {
		t.Fatalf("example: %+v", cfg)
	}
	r := cfg.Runnables[0]
	if r.Checkpoint.MaxSize != 256*KiB || r.Restart.Max != 2*time.Minute || r.Config["batch"] != 100 {
		t.Fatalf("example runnable: %+v", r)
	}
}

func TestDefaultsLAN(t *testing.T) {
	cfg := mustLoad(t, valid)
	want := Raft{Profile: ProfileLAN, HeartbeatTimeout: time.Second, ElectionTimeout: time.Second,
		LeaderLeaseTimeout: 500 * time.Millisecond, SnapshotThreshold: 512, TrailingLogs: 256}
	if cfg.Raft != want {
		t.Fatalf("raft %+v, want %+v", cfg.Raft, want)
	}
	if cfg.Lease != (Lease{Duration: 750 * time.Millisecond, RenewInterval: 250 * time.Millisecond}) {
		t.Fatalf("lease %+v", cfg.Lease)
	}
	r := cfg.Runnables[0]
	if r.Kind != KindService || r.StopGrace != 2*time.Second || r.ShutdownGrace != 30*time.Second ||
		r.Restart != (Restart{Initial: time.Second, Max: 2 * time.Minute, HandoverAfter: 5}) ||
		r.Checkpoint != (Checkpoint{MaxSize: 256 * KiB, MinInterval: time.Second}) ||
		r.Health.Timeout != 30*time.Second {
		t.Fatalf("runnable defaults %+v", r)
	}
	if r.Store != (Store{MaxBatch: MiB, MaxSteps: 50_000_000, MaxSize: 256 * MiB, MaxResult: MiB, QueryTimeout: 30 * time.Second}) {
		t.Fatalf("store defaults %+v", r.Store)
	}
	if cfg.Admin.Socket != "/run/resurgo/admin.sock" {
		t.Fatalf("admin socket %q", cfg.Admin.Socket)
	}
	if d := cfg.TakeoverDelay(r); d != 2250*time.Millisecond {
		t.Fatalf("takeover delay auto %s, want 2.25s", d)
	}
}

func TestDefaultsWAN(t *testing.T) {
	cfg := mustLoad(t, valid+"raft: { profile: wan }\n")
	if cfg.Raft.HeartbeatTimeout != 2*time.Second || cfg.Raft.ElectionTimeout != 2*time.Second || cfg.Raft.LeaderLeaseTimeout != time.Second {
		t.Fatalf("raft %+v", cfg.Raft)
	}
	if cfg.Lease != (Lease{Duration: 1500 * time.Millisecond, RenewInterval: 400 * time.Millisecond}) {
		t.Fatalf("lease %+v", cfg.Lease)
	}
	if d := cfg.TakeoverDelay(cfg.Runnables[0]); d != 2*time.Second {
		t.Fatalf("takeover delay auto %s, want 2s", d)
	}
}

func TestOverrides(t *testing.T) {
	cfg := mustLoad(t, valid+`raft: { heartbeat_timeout: 3s, leader_lease_timeout: 1s }
lease: { duration: 2s, renew_interval: 500ms }
`)
	if cfg.Raft.HeartbeatTimeout != 3*time.Second || cfg.Raft.ElectionTimeout != time.Second {
		t.Fatalf("raft %+v", cfg.Raft)
	}
	cfg = mustLoad(t, strings.Replace(valid, "    sha256:", "    takeover_delay: 0s\n    kind: task\n    sha256:", 1))
	if d := cfg.TakeoverDelay(cfg.Runnables[0]); d != 0 {
		t.Fatalf("explicit takeover delay %s, want 0", d)
	}
	if cfg.Runnables[0].Kind != KindTask {
		t.Fatalf("kind %q", cfg.Runnables[0].Kind)
	}
	// a long grace makes auto longer, a short one is floored at 0
	cfg = mustLoad(t, strings.Replace(valid, "    sha256:", "    stop_grace: 100ms\n    sha256:", 1))
	if d := cfg.TakeoverDelay(cfg.Runnables[0]); d != 350*time.Millisecond {
		t.Fatalf("takeover delay %s, want 350ms", d)
	}
}

func TestRefused(t *testing.T) {
	for _, tc := range []struct {
		name, from, to, want string
	}{
		{"unknown field", "node:\n", "node:\n  colour: blue\n", "colour"},
		{"no node id", "  id: node-a\n  data_dir", "  data_dir", "node.id"},
		{"relative data dir", "/var/lib/resurgo", "var/lib/resurgo", "data_dir"},
		{"duplicate voter", "node-c, address: 10.0.1.13", "node-b, address: 10.0.1.13", "twice"},
		{"duplicate address", "10.0.1.13:7350 }", "10.0.1.12:7350 }", "twice"},
		{"voter without identity", "      node-c: node-c.example.org\n", "", "node-c"},
		{"bad identity", "node-c.example.org", "http://node-c", "identity"},
		{"bad address", "10.0.1.13:7350 }", "10.0.1.13 }", "address"},
		{"unspecified advertise", "advertise: 10.0.1.11:7350", "advertise: 0.0.0.0:7350", "advertise"},
		{"bad digest", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "abc", "sha256"},
		{"bad runnable name", "name: svc", "name: ../svc", "name"},
		{"bad profile", "runnables:", "raft: { profile: moon }\nrunnables:", "profile"},
		{"lease not below heartbeat", "runnables:", "lease: { duration: 1s }\nrunnables:", "lease.duration"},
		{"leader lease not below heartbeat", "runnables:", "raft: { leader_lease_timeout: 1s }\nrunnables:", "leader_lease_timeout"},
		{"renew too slow", "runnables:", "lease: { renew_interval: 300ms }\nrunnables:", "renew_interval"},
		{"checkpoint too large", "    sha256:", "    checkpoint: { max_size: 2MiB }\n    sha256:", "max_size"},
		{"batch too large", "    sha256:", "    store: { max_batch: 5MiB }\n    sha256:", "max_batch"},
		{"bad size", "    sha256:", "    checkpoint: { max_size: 1 parsec }\n    sha256:", "size"},
		{"bad kind", "    sha256:", "    kind: daemon\n    sha256:", "kind"},
		{"bad takeover delay", "    sha256:", "    takeover_delay: soon\n    sha256:", "takeover_delay"},
		{"relative admin socket", "runnables:", "admin: { socket: admin.sock }\nrunnables:", "admin.socket"},
		{"remote without identities", "runnables:", strings.Replace("admin:\n  remote:\n    listen: 0.0.0.0:7351\n    tls: { cert: /etc/resurgo/admin.crt, key: /etc/resurgo/admin.key, ca: /etc/resurgo/admin-ca.crt }\n    allowed_identities: [\"spiffe://example.org/ops\"]\nrunnables:", "[\"spiffe://example.org/ops\"]", "[]", 1), "allowed_identities"},
		{"remote with a bad identity", "runnables:", strings.Replace("admin:\n  remote:\n    listen: 0.0.0.0:7351\n    tls: { cert: /etc/resurgo/admin.crt, key: /etc/resurgo/admin.key, ca: /etc/resurgo/admin-ca.crt }\n    allowed_identities: [\"spiffe://example.org/ops\"]\nrunnables:", "spiffe://example.org/ops", "http://ops", 1), "allowed_identities"},
		{"remote with a bad listen", "runnables:", strings.Replace("admin:\n  remote:\n    listen: 0.0.0.0:7351\n    tls: { cert: /etc/resurgo/admin.crt, key: /etc/resurgo/admin.key, ca: /etc/resurgo/admin-ca.crt }\n    allowed_identities: [\"spiffe://example.org/ops\"]\nrunnables:", "0.0.0.0:7351", "7351", 1), "admin.remote.listen"},
		{"remote with a relative cert", "runnables:", strings.Replace("admin:\n  remote:\n    listen: 0.0.0.0:7351\n    tls: { cert: /etc/resurgo/admin.crt, key: /etc/resurgo/admin.key, ca: /etc/resurgo/admin-ca.crt }\n    allowed_identities: [\"spiffe://example.org/ops\"]\nrunnables:", "/etc/resurgo/admin.crt", "admin.crt", 1), "admin.remote.tls.cert"},
		{"duplicate runnable", "runnables:\n", "runnables:\n  - { name: svc, binary: /x, sha256: " + strings.Repeat("0", 64) + " }\n", "twice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := strings.Replace(valid, tc.from, tc.to, 1)
			if text == valid {
				t.Fatalf("test case does not change the configuration")
			}
			_, err := load(t, text)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want one mentioning %q", err, tc.want)
			}
		})
	}
}

func TestRemoteAdmin(t *testing.T) {
	cfg := mustLoad(t, strings.Replace(valid, "runnables:", "admin:\n  remote:\n    listen: 0.0.0.0:7351\n    tls: { cert: /etc/resurgo/admin.crt, key: /etc/resurgo/admin.key, ca: /etc/resurgo/admin-ca.crt }\n    allowed_identities: [\"spiffe://example.org/ops\"]\nrunnables:", 1))
	if r := cfg.Admin.Remote; r == nil || r.Listen != "0.0.0.0:7351" || r.TLS.CA != "/etc/resurgo/admin-ca.crt" || len(r.AllowedIdentities) != 1 {
		t.Fatalf("remote admin %+v", cfg.Admin.Remote)
	}
	if mustLoad(t, valid).Admin.Remote != nil {
		t.Fatal("the remote admin endpoint is on without being configured")
	}
}

func TestAllProblemsReported(t *testing.T) {
	text := strings.Replace(strings.Replace(valid, "  id: node-a\n  data_dir", "  data_dir", 1), "/var/lib/resurgo", "relative", 1)
	_, err := load(t, text)
	if err == nil || !strings.Contains(err.Error(), "node.id") || !strings.Contains(err.Error(), "data_dir") {
		t.Fatalf("error %v, want both problems", err)
	}
}

func TestByteSize(t *testing.T) {
	for in, want := range map[string]ByteSize{"0": 0, "512": 512, "1B": 1, "256KiB": 256 * KiB, "4MiB": 4 * MiB, "1GiB": GiB} {
		var b ByteSize
		if err := b.UnmarshalText([]byte(in)); err != nil || b != want {
			t.Errorf("%q: %d %v, want %d", in, b, err, want)
		}
	}
	for _, in := range []string{"", "KiB", "-1", "1.5MiB", "1MB", "1 KiB"} {
		var b ByteSize
		if err := b.UnmarshalText([]byte(in)); err == nil {
			t.Errorf("%q accepted as %d", in, b)
		}
	}
}
