// Package tlstest issues test certificates: a CA and node certificates
// carrying a SPIFFE ID URI SAN or a DNS SAN, as the cluster port expects
// (SEC-2). For tests and the lab only.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// CA is a certificate authority held in memory.
type CA struct {
	Cert *x509.Certificate
	key  *ecdsa.PrivateKey
	// PEM is the CA certificate, PEM-encoded.
	PEM []byte
}

// NewCA creates a CA valid for a day.
func NewCA(t testing.TB) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlstest: CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial(t),
		Subject:               pkix.Name{CommonName: "drun test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("tlstest: CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("tlstest: CA certificate: %v", err)
	}
	return &CA{Cert: cert, key: key, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// Issue returns a certificate and key (PEM) for a node identity: a
// spiffe:// URI or a DNS name, usable as client and as server.
func (ca *CA) Issue(t testing.TB, identity string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlstest: key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(t),
		Subject:      pkix.Name{CommonName: identity},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	if strings.HasPrefix(identity, "spiffe://") {
		u, err := url.Parse(identity)
		if err != nil {
			t.Fatalf("tlstest: identity %q: %v", identity, err)
		}
		tmpl.URIs = []*url.URL{u}
	} else {
		tmpl.DNSNames = []string{identity}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("tlstest: certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("tlstest: key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// Files are the paths of a node's certificate, key and CA.
type Files struct{ Cert, Key, CA string }

// WriteFiles issues a certificate for identity and writes it, its key and
// the CA into dir.
func (ca *CA) WriteFiles(t testing.TB, dir, identity string) Files {
	t.Helper()
	certPEM, keyPEM := ca.Issue(t, identity)
	f := Files{Cert: filepath.Join(dir, "node.crt"), Key: filepath.Join(dir, "node.key"), CA: filepath.Join(dir, "ca.crt")}
	for path, data := range map[string][]byte{f.Cert: certPEM, f.Key: keyPEM, f.CA: ca.PEM} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("tlstest: %v", err)
		}
	}
	return f
}

func serial(t testing.TB) *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("tlstest: serial: %v", err)
	}
	return n
}
