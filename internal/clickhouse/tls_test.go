package clickhouse

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testPKI issues a private root CA and a server certificate for host,
// mirroring a company CA that is not in the system trust store.
func testPKI(t *testing.T, host string) (caPEM []byte, server tls.Certificate) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Company Root CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	srvKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	srvTpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: host}, DNSNames: []string{host},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTpl, ca, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		tls.Certificate{Certificate: [][]byte{srvDER}, PrivateKey: srvKey}
}

// handshake runs a real TLS handshake over a loopback TCP connection (a
// synchronous net.Pipe can deadlock on the alert a failed handshake sends).
func handshake(t *testing.T, client *tls.Config, server tls.Certificate) error {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{server}})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		_ = conn.(*tls.Conn).Handshake()
		_ = conn.Close()
	}()
	raw, err := net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
	return tls.Client(raw, client).Handshake()
}

func TestTLS_PasswordAuthWithCompanyCA(t *testing.T) {
	caPEM, srv := testPKI(t, "ch-1.prod.internal")
	caFile := filepath.Join(t.TempDir(), "company-root.crt")
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	// password auth: CA only, no client certificate
	base, err := buildTLS(&TLSFiles{CAFile: caFile})
	if err != nil {
		t.Fatalf("buildTLS without client cert: %v", err)
	}
	if len(base.Certificates) != 0 {
		t.Fatal("no client certificate expected for password auth")
	}
	if err := handshake(t, tlsForHost(base, "ch-1.prod.internal"), srv); err != nil {
		t.Fatalf("handshake with company CA failed: %v", err)
	}
	// the certificate must match the endpoint we dialled
	if err := handshake(t, tlsForHost(base, "ch-2.prod.internal"), srv); err == nil {
		t.Fatal("handshake succeeded against a certificate for another host")
	}
	if base.ServerName != "" {
		t.Fatal("tlsForHost mutated the shared base config")
	}
}

func TestTLS_UnknownCAIsRejected(t *testing.T) {
	_, srv := testPKI(t, "ch-1.prod.internal")
	otherCA, _ := testPKI(t, "ch-1.prod.internal") // different root
	caFile := filepath.Join(t.TempDir(), "other.crt")
	if err := os.WriteFile(caFile, otherCA, 0o600); err != nil {
		t.Fatal(err)
	}
	base, err := buildTLS(&TLSFiles{CAFile: caFile})
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(t, tlsForHost(base, "ch-1.prod.internal"), srv); err == nil {
		t.Fatal("server certificate from an untrusted CA was accepted")
	}
}

func TestBuildTLS_Errors(t *testing.T) {
	if cfg, err := buildTLS(nil); cfg != nil || err != nil {
		t.Fatalf("nil TLSFiles must mean plaintext, got %v %v", cfg, err)
	}
	if _, err := buildTLS(&TLSFiles{CAFile: "/does/not/exist.crt"}); err == nil || !strings.Contains(err.Error(), "read CA file") {
		t.Fatalf("want read CA error, got %v", err)
	}
	junk := filepath.Join(t.TempDir(), "junk.crt")
	_ = os.WriteFile(junk, []byte("not a pem"), 0o600)
	if _, err := buildTLS(&TLSFiles{CAFile: junk}); err == nil || !strings.Contains(err.Error(), "no valid certificates") {
		t.Fatalf("want invalid CA error, got %v", err)
	}
	if _, err := buildTLS(&TLSFiles{CertFile: "/x.crt"}); err == nil || !strings.Contains(err.Error(), "client certificate") {
		t.Fatalf("half a key pair must fail, got %v", err)
	}
	if got := tlsForHost(nil, "h"); got != nil {
		t.Fatal("tlsForHost(nil) must stay nil")
	}
}
