package raftnode

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// IdentitySource gives the node's certificate and the roots it trusts,
// current at each handshake (SEC-3). FileIdentity is the one source for
// now; the SPIFFE Workload API would be another (OI-4).
type IdentitySource interface {
	Certificate() (*tls.Certificate, error)
	Roots() (*x509.CertPool, error)
}

// FileIdentity reads PEM files and reads them again when one changes
// (size or modification time), checked at each handshake. Files that do
// not load (a key replaced before its certificate) leave the last good
// identity in use, and are logged once.
type FileIdentity struct {
	cert, key, ca string
	logger        *slog.Logger

	mu     sync.Mutex
	stamp  string // of the files loaded
	failed string // of the files that last failed to load
	pair   *tls.Certificate
	roots  *x509.CertPool
}

// NewFileIdentity loads the certificate, key and CA files; they must load
// now.
func NewFileIdentity(cert, key, ca string, logger *slog.Logger) (*FileIdentity, error) {
	f := &FileIdentity{cert: cert, key: key, ca: ca, logger: logger}
	if err := f.reload(); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *FileIdentity) Certificate() (*tls.Certificate, error) {
	f.refresh()
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pair, nil
}

func (f *FileIdentity) Roots() (*x509.CertPool, error) {
	f.refresh()
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.roots, nil
}

// refresh reloads changed files, keeping the last good identity on failure
func (f *FileIdentity) refresh() {
	if err := f.reload(); err != nil {
		f.logger.Warn("identity files do not load: keeping the previous identity", "error", err)
	}
}

// reload reads the files if they changed; a failure is reported once per
// change of the files
func (f *FileIdentity) reload() error {
	var stamp strings.Builder
	for _, path := range []string{f.cert, f.key, f.ca} {
		fi, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("identity: %w", err)
		}
		fmt.Fprintf(&stamp, "%d/%d;", fi.Size(), fi.ModTime().UnixNano())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if stamp.String() == f.stamp || stamp.String() == f.failed {
		return nil
	}
	pair, roots, err := loadIdentity(f.cert, f.key, f.ca)
	if err != nil {
		f.failed = stamp.String()
		return err
	}
	f.stamp, f.failed, f.pair, f.roots = stamp.String(), "", pair, roots
	return nil
}

func loadIdentity(cert, key, ca string) (*tls.Certificate, *x509.CertPool, error) {
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, nil, fmt.Errorf("identity: %w", err)
	}
	caPEM, err := os.ReadFile(ca) //nolint:gosec // the configured CA file
	if err != nil {
		return nil, nil, fmt.Errorf("identity: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("identity: no certificate in %s", ca)
	}
	return &pair, roots, nil
}

// Peers maps node IDs to the identities their certificates carry (SEC-2).
type Peers map[string]string

// ID returns the one node whose identity the certificate carries.
func (p Peers) ID(cert *x509.Certificate) (string, error) {
	var found []string
	for id, identity := range p {
		if hasIdentity(cert, identity) {
			found = append(found, id)
		}
	}
	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return "", errors.New("the certificate carries no configured peer identity")
	default:
		return "", fmt.Errorf("the certificate carries the identities of several nodes: %v", found)
	}
}

// hasIdentity: a spiffe:// identity is a URI SAN, anything else a DNS SAN
func hasIdentity(cert *x509.Certificate, identity string) bool {
	if strings.HasPrefix(identity, "spiffe://") {
		for _, u := range cert.URIs {
			if u.String() == identity {
				return true
			}
		}
		return false
	}
	for _, name := range cert.DNSNames {
		if strings.EqualFold(name, identity) {
			return true
		}
	}
	return false
}

// verifyChain checks the peer's chain against roots, for usage
func verifyChain(cs tls.ConnectionState, roots *x509.CertPool, usage x509.ExtKeyUsage) (*x509.Certificate, error) {
	if len(cs.PeerCertificates) == 0 {
		return nil, errors.New("no peer certificate")
	}
	inter := x509.NewCertPool()
	for _, c := range cs.PeerCertificates[1:] {
		inter.AddCert(c)
	}
	leaf := cs.PeerCertificates[0]
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{usage}, CurrentTime: time.Now()}); err != nil {
		return nil, fmt.Errorf("peer certificate: %w", err)
	}
	return leaf, nil
}
