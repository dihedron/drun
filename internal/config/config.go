// Package config loads and checks a node's configuration file
// (resurgo_specification.md §10.1), with the defaults and constraints of
// §8.3, §8.4, §8.6 and §8.7.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"go.yaml.in/yaml/v3"
)

// Profiles of Raft and lease timings (§8.6, §8.7).
const (
	ProfileLAN = "lan"
	ProfileWAN = "wan"
)

// Kinds of runnable (FR-7).
const (
	KindService = "service"
	KindTask    = "task"
)

// limits that no configuration may exceed
const (
	maxCheckpointSize = MiB     // FR-9
	maxBatch          = 4 * MiB // STO-7
	driftMargin       = 500 * time.Millisecond
)

// Config is a node's configuration.
type Config struct {
	Node      Node       `yaml:"node"`
	Cluster   Cluster    `yaml:"cluster"`
	Raft      Raft       `yaml:"raft"`
	Lease     Lease      `yaml:"lease"`
	Admin     Admin      `yaml:"admin"`
	Metrics   Metrics    `yaml:"metrics"`
	Runnables []Runnable `yaml:"runnables"`
}

type Node struct {
	// ID is the Raft server ID: unique and stable.
	ID      string `yaml:"id"`
	DataDir string `yaml:"data_dir"`
}

type Cluster struct {
	Name      string `yaml:"name"`
	Bind      string `yaml:"bind"`
	Advertise string `yaml:"advertise"`
	// Voters are the initial voters, identical on all of them (FR-1).
	Voters []Server `yaml:"voters"`
	TLS    TLS      `yaml:"tls"`
}

type Server struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}

type TLS struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
	CA   string `yaml:"ca"`
	// Peers maps every node ID to the identity its certificate MUST carry:
	// a spiffe:// URI SAN or a DNS SAN (SEC-2).
	Peers map[string]string `yaml:"peers"`
}

type Raft struct {
	Profile            string        `yaml:"profile"`
	HeartbeatTimeout   time.Duration `yaml:"heartbeat_timeout"`
	ElectionTimeout    time.Duration `yaml:"election_timeout"`
	LeaderLeaseTimeout time.Duration `yaml:"leader_lease_timeout"`
	SnapshotThreshold  uint64        `yaml:"snapshot_threshold"`
	TrailingLogs       uint64        `yaml:"trailing_logs"`
}

type Lease struct {
	Duration      time.Duration `yaml:"duration"`
	RenewInterval time.Duration `yaml:"renew_interval"`
}

type Admin struct {
	Socket        string   `yaml:"socket"`
	AllowedGroups []string `yaml:"allowed_groups"`
	// Remote, if set, also serves the admin API on a TCP endpoint with
	// mutual TLS (SEC-11); absent, the API is local only.
	Remote *Remote `yaml:"remote"`
}

// Remote is the opt-in remote admin endpoint.
type Remote struct {
	Listen string   `yaml:"listen"`
	TLS    TLSFiles `yaml:"tls"`
	// AllowedIdentities are the client identities (spiffe:// URI or DNS
	// SAN) admitted; every other client is refused.
	AllowedIdentities []string `yaml:"allowed_identities"`
}

// TLSFiles are a certificate, its key and the CA that signs the peers'.
type TLSFiles struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
	CA   string `yaml:"ca"`
}

type Metrics struct {
	Listen string `yaml:"listen"`
}

type Runnable struct {
	Name   string `yaml:"name"`
	Kind   string `yaml:"kind"`
	Binary string `yaml:"binary"`
	SHA256 string `yaml:"sha256"`
	// Config is passed to Validate and Run as JSON.
	Config        map[string]any `yaml:"config"`
	TakeoverDelay TakeoverDelay  `yaml:"takeover_delay"`
	StopGrace     time.Duration  `yaml:"stop_grace"`
	ShutdownGrace time.Duration  `yaml:"shutdown_grace"`
	Restart       Restart        `yaml:"restart"`
	Checkpoint    Checkpoint     `yaml:"checkpoint"`
	Health        Health         `yaml:"health"`
	Store         Store          `yaml:"store"`
}

type Restart struct {
	Initial       time.Duration `yaml:"initial"`
	Max           time.Duration `yaml:"max"`
	HandoverAfter int           `yaml:"handover_after"`
}

type Checkpoint struct {
	MaxSize     ByteSize      `yaml:"max_size"`
	MinInterval time.Duration `yaml:"min_interval"`
}

type Health struct {
	Timeout time.Duration `yaml:"timeout"`
}

// Store holds the limits of the runnable's store (resurgo-store.md STO-7
// to STO-11).
type Store struct {
	MaxBatch     ByteSize      `yaml:"max_batch"`
	MaxSteps     uint64        `yaml:"max_steps"`
	MaxSize      ByteSize      `yaml:"max_size"`
	MaxResult    ByteSize      `yaml:"max_result"`
	QueryTimeout time.Duration `yaml:"query_timeout"`
}

// Load reads, completes with the defaults and checks a configuration file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // the operator's configuration file
	if err != nil {
		return nil, fmt.Errorf("reading the configuration: %w", err)
	}
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("configuration %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("configuration %s: %w", path, err)
	}
	return &cfg, nil
}

func setDefault[T comparable](v *T, def T) {
	var zero T
	if *v == zero {
		*v = def
	}
}

func (c *Config) applyDefaults() {
	setDefault(&c.Raft.Profile, ProfileLAN)
	// unknown profiles are refused by Validate; LAN values meanwhile
	hb, lease, renew := time.Second, 750*time.Millisecond, 250*time.Millisecond
	if c.Raft.Profile == ProfileWAN {
		hb, lease, renew = 2*time.Second, 1500*time.Millisecond, 400*time.Millisecond
	}
	setDefault(&c.Raft.HeartbeatTimeout, hb)
	setDefault(&c.Raft.ElectionTimeout, hb)
	setDefault(&c.Raft.LeaderLeaseTimeout, hb/2)
	setDefault(&c.Raft.SnapshotThreshold, 512)
	setDefault(&c.Raft.TrailingLogs, 256)
	setDefault(&c.Lease.Duration, lease)
	setDefault(&c.Lease.RenewInterval, renew)
	setDefault(&c.Admin.Socket, "/run/resurgo/admin.sock")
	for i := range c.Runnables {
		r := &c.Runnables[i]
		setDefault(&r.Kind, KindService)
		setDefault(&r.StopGrace, 2*time.Second)
		setDefault(&r.ShutdownGrace, 30*time.Second)
		setDefault(&r.Restart.Initial, time.Second)
		setDefault(&r.Restart.Max, 2*time.Minute)
		setDefault(&r.Restart.HandoverAfter, 5)
		setDefault(&r.Checkpoint.MaxSize, 256*KiB)
		setDefault(&r.Checkpoint.MinInterval, time.Second)
		setDefault(&r.Health.Timeout, 30*time.Second)
		setDefault(&r.Store.MaxBatch, MiB)
		setDefault(&r.Store.MaxSteps, 50_000_000)
		setDefault(&r.Store.MaxSize, 256*MiB)
		setDefault(&r.Store.MaxResult, MiB)
		setDefault(&r.Store.QueryTimeout, 30*time.Second)
	}
}

var (
	// a runnable's name is also its store's file name
	runnableName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	sha256Hex    = regexp.MustCompile(`^[0-9a-f]{64}$`)
	dnsName      = regexp.MustCompile(`^([A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)
)

// problems collects the problems of a configuration
type problems []error

func (p *problems) bad(format string, args ...any) { *p = append(*p, fmt.Errorf(format, args...)) }

// Validate reports every problem of the configuration at once.
func (c *Config) Validate() error {
	var p problems
	c.validateCluster(p.bad)
	c.validateTLS(p.bad)
	c.validateTimings(p.bad)
	c.validateAdmin(p.bad)
	c.validateRunnables(p.bad)
	return errors.Join(p...)
}

func (c *Config) validateCluster(bad func(string, ...any)) {
	if c.Node.ID == "" {
		bad("node.id is missing")
	}
	if !filepath.IsAbs(c.Node.DataDir) {
		bad("node.data_dir %q is not an absolute path", c.Node.DataDir)
	}
	if c.Cluster.Name == "" {
		bad("cluster.name is missing")
	}
	if _, _, err := net.SplitHostPort(c.Cluster.Bind); err != nil {
		bad("cluster.bind %q: %w", c.Cluster.Bind, err)
	}
	if host, _, err := net.SplitHostPort(c.Cluster.Advertise); err != nil {
		bad("cluster.advertise %q: %w", c.Cluster.Advertise, err)
	} else if ip := net.ParseIP(host); host == "" || ip != nil && ip.IsUnspecified() {
		bad("cluster.advertise %q: other nodes cannot reach an unspecified address", c.Cluster.Advertise)
	}

	ids, addrs := map[string]bool{}, map[string]bool{}
	for _, v := range c.Cluster.Voters {
		if v.ID == "" {
			bad("cluster.voters: a voter without id")
		} else if ids[v.ID] {
			bad("cluster.voters: %q twice", v.ID)
		}
		if _, _, err := net.SplitHostPort(v.Address); err != nil {
			bad("cluster.voters: %q has address %q: %w", v.ID, v.Address, err)
		} else if addrs[v.Address] {
			bad("cluster.voters: address %q twice", v.Address)
		}
		ids[v.ID], addrs[v.Address] = true, true
		if v.ID == c.Node.ID && v.Address != c.Cluster.Advertise {
			bad("cluster.voters: %q has address %q, but cluster.advertise is %q", v.ID, v.Address, c.Cluster.Advertise)
		}
	}
	if len(c.Cluster.Voters) == 0 {
		bad("cluster.voters is empty")
	}
}

func (c *Config) validateTLS(bad func(string, ...any)) {
	for _, f := range []struct{ name, path string }{{"cert", c.Cluster.TLS.Cert}, {"key", c.Cluster.TLS.Key}, {"ca", c.Cluster.TLS.CA}} {
		if !filepath.IsAbs(f.path) {
			bad("cluster.tls.%s %q is not an absolute path", f.name, f.path)
		}
	}
	for id, identity := range c.Cluster.TLS.Peers {
		if err := ValidIdentity(identity); err != nil {
			bad("cluster.tls.peers: %q: %w", id, err)
		}
	}
	need := []string{c.Node.ID}
	for _, v := range c.Cluster.Voters {
		need = append(need, v.ID)
	}
	for _, id := range need {
		if _, ok := c.Cluster.TLS.Peers[id]; id != "" && !ok {
			bad("cluster.tls.peers: no identity for %q", id)
		}
	}
}

func (c *Config) validateAdmin(bad func(string, ...any)) {
	if l := c.Metrics.Listen; l != "" {
		if _, _, err := net.SplitHostPort(l); err != nil {
			bad("metrics.listen %q: %w", l, err)
		}
	}
	if !filepath.IsAbs(c.Admin.Socket) {
		bad("admin.socket %q is not an absolute path", c.Admin.Socket)
	}
	r := c.Admin.Remote
	if r == nil {
		return
	}
	if _, _, err := net.SplitHostPort(r.Listen); err != nil {
		bad("admin.remote.listen %q: %w", r.Listen, err)
	}
	for _, f := range []struct{ name, path string }{{"cert", r.TLS.Cert}, {"key", r.TLS.Key}, {"ca", r.TLS.CA}} {
		if !filepath.IsAbs(f.path) {
			bad("admin.remote.tls.%s %q is not an absolute path", f.name, f.path)
		}
	}
	if len(r.AllowedIdentities) == 0 {
		bad("admin.remote.allowed_identities is empty: no client could connect")
	}
	for _, identity := range r.AllowedIdentities {
		if err := ValidIdentity(identity); err != nil {
			bad("admin.remote.allowed_identities: %w", err)
		}
	}
}

func (c *Config) validateTimings(bad func(string, ...any)) {
	switch c.Raft.Profile {
	case ProfileLAN, ProfileWAN:
	default:
		bad("raft.profile %q is neither %q nor %q", c.Raft.Profile, ProfileLAN, ProfileWAN)
	}
	if c.Raft.LeaderLeaseTimeout >= c.Raft.HeartbeatTimeout {
		bad("raft.leader_lease_timeout %s must be less than raft.heartbeat_timeout %s", c.Raft.LeaderLeaseTimeout, c.Raft.HeartbeatTimeout)
	}
	if c.Lease.Duration >= c.Raft.HeartbeatTimeout {
		bad("lease.duration %s must be less than raft.heartbeat_timeout %s (§8.3)", c.Lease.Duration, c.Raft.HeartbeatTimeout)
	}
	if c.Lease.RenewInterval > c.Lease.Duration/3 {
		bad("lease.renew_interval %s must be at most a third of lease.duration %s", c.Lease.RenewInterval, c.Lease.Duration)
	}
}

func (c *Config) validateRunnables(bad func(string, ...any)) {
	names := map[string]bool{}
	for _, r := range c.Runnables {
		if !runnableName.MatchString(r.Name) {
			bad("runnables: name %q: letters, digits, '.', '_' and '-' only, at most 64", r.Name)
		} else if names[r.Name] {
			bad("runnables: %q twice", r.Name)
		}
		names[r.Name] = true
		if r.Kind != KindService && r.Kind != KindTask {
			bad("runnables: %q: kind %q is neither %q nor %q", r.Name, r.Kind, KindService, KindTask)
		}
		if !filepath.IsAbs(r.Binary) {
			bad("runnables: %q: binary %q is not an absolute path", r.Name, r.Binary)
		}
		if !sha256Hex.MatchString(r.SHA256) {
			bad("runnables: %q: sha256 must be 64 lower-case hexadecimal digits", r.Name)
		}
		if r.Checkpoint.MaxSize > maxCheckpointSize {
			bad("runnables: %q: checkpoint.max_size %d is more than %d", r.Name, r.Checkpoint.MaxSize, maxCheckpointSize)
		}
		if r.Store.MaxBatch > maxBatch {
			bad("runnables: %q: store.max_batch %d is more than %d", r.Name, r.Store.MaxBatch, maxBatch)
		}
		if r.Restart.Initial > r.Restart.Max {
			bad("runnables: %q: restart.initial %s is more than restart.max %s", r.Name, r.Restart.Initial, r.Restart.Max)
		}
	}
}

// ValidRunnableName checks a runnable's name, which is also its store's
// file name.
func ValidRunnableName(name string) error {
	if !runnableName.MatchString(name) {
		return fmt.Errorf("runnable name %q: letters, digits, '.', '_' and '-' only, at most 64", name)
	}
	return nil
}

// ValidIdentity checks a peer identity: a spiffe:// URI or a DNS name.
func ValidIdentity(identity string) error {
	if u, err := url.Parse(identity); err == nil && u.Scheme == "spiffe" {
		if u.Host == "" {
			return fmt.Errorf("identity %q: a SPIFFE ID needs a trust domain", identity)
		}
		return nil
	}
	if !dnsName.MatchString(identity) {
		return fmt.Errorf("identity %q is neither a spiffe:// URI nor a DNS name", identity)
	}
	return nil
}

// TakeoverDelay is the runnable's delay: the configured one, or, for auto,
// the smallest that leaves no overlap (§8.4).
func (c *Config) TakeoverDelay(r Runnable) time.Duration {
	if d, ok := r.TakeoverDelay.Fixed(); ok {
		return d
	}
	return max(0, c.Lease.Duration+r.StopGrace-c.Raft.HeartbeatTimeout+driftMargin)
}
