// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcs

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"fmt"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeTestKeyPair writes a self-signed certificate and its private key to dir
// and returns the cert path and key path.
func writeTestKeyPair(t *testing.T, dir, name string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

func TestBuildTLSConfigPlaintext(t *testing.T) {
	cfg, err := buildTLSConfig("", "", "")
	if err != nil {
		t.Fatalf("buildTLSConfig() error = %v, want nil", err)
	}
	if cfg != nil {
		t.Fatalf("buildTLSConfig() = %v, want nil (plaintext)", cfg)
	}
}

func TestBuildTLSConfigServerOnly(t *testing.T) {
	caPath, _ := writeTestKeyPair(t, t.TempDir(), "ca")
	cfg, err := buildTLSConfig(caPath, "", "")
	if err != nil {
		t.Fatalf("buildTLSConfig() error = %v", err)
	}
	if cfg == nil {
		t.Fatal("buildTLSConfig() = nil, want non-nil TLS config")
	}
	if len(cfg.Certificates) != 0 {
		t.Fatalf("got %d client certificates, want 0 (server-only TLS)", len(cfg.Certificates))
	}
	if cfg.RootCAs == nil {
		t.Fatal("RootCAs is nil, want CA pool")
	}
}

func TestBuildTLSConfigMutualTLS(t *testing.T) {
	dir := t.TempDir()
	caPath, _ := writeTestKeyPair(t, dir, "ca")
	certPath, keyPath := writeTestKeyPair(t, dir, "client")
	cfg, err := buildTLSConfig(caPath, certPath, keyPath)
	if err != nil {
		t.Fatalf("buildTLSConfig() error = %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("got %d client certificates, want 1 (mutual TLS)", len(cfg.Certificates))
	}
}

func TestBuildTLSConfigRequiresBothKeyAndCert(t *testing.T) {
	dir := t.TempDir()
	caPath, _ := writeTestKeyPair(t, dir, "ca")
	certPath, _ := writeTestKeyPair(t, dir, "client")
	_, err := buildTLSConfig(caPath, certPath, "")
	if err == nil || !strings.Contains(err.Error(), "requires both CertFile and KeyFile") {
		t.Fatalf("buildTLSConfig() error = %v, want 'requires both CertFile and KeyFile'", err)
	}
	_, err = buildTLSConfig(caPath, "", certPath)
	if err == nil || !strings.Contains(err.Error(), "requires both CertFile and KeyFile") {
		t.Fatalf("buildTLSConfig() error = %v, want 'requires both CertFile and KeyFile'", err)
	}
}

func TestBuildTLSConfigMissingCAFile(t *testing.T) {
	_, err := buildTLSConfig(filepath.Join(t.TempDir(), "missing.pem"), "", "")
	if err == nil {
		t.Fatal("buildTLSConfig() = nil error, want load error for missing CA file")
	}
}

func TestBuildTLSConfigInvalidCAPEM(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bad.pem")
	if err := os.WriteFile(p, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write bad pem: %v", err)
	}
	_, err := buildTLSConfig(p, "", "")
	if err == nil || !strings.Contains(err.Error(), "no certificates found") {
		t.Fatalf("buildTLSConfig() error = %v, want 'no certificates found'", err)
	}
}

func TestDialOptionsOverride(t *testing.T) {
	base := DialOptions{CAFile: "env-ca", Token: "env-token", ConnectTimeoutMs: 5000}
	got := base.Override(DialOptions{CertFile: "explicit-cert", Token: "explicit-token"})
	if got.CAFile != "env-ca" {
		t.Fatalf("Override() CAFile = %q, want env-ca preserved (empty not clobbered)", got.CAFile)
	}
	if got.CertFile != "explicit-cert" {
		t.Fatalf("Override() CertFile = %q, want explicit-cert", got.CertFile)
	}
	if got.Token != "explicit-token" {
		t.Fatalf("Override() Token = %q, want explicit-token", got.Token)
	}
	if got.ConnectTimeoutMs != 5000 {
		t.Fatalf("Override() ConnectTimeoutMs = %d, want 5000 preserved (zero not clobbered)", got.ConnectTimeoutMs)
	}

	zero := DialOptions{}.Override(DialOptions{CAFile: "", CertFile: "x", ConnectTimeoutMs: 0})
	if zero.CAFile != "" || zero.ConnectTimeoutMs != 0 {
		t.Fatalf("Override() zero base = %+v, want empty fields skipped", zero)
	}
}

func TestOverrideKeepsASetDialer(t *testing.T) {
	dialer := func(ctx context.Context, addr string) (net.Conn, error) { return nil, io.ErrClosedPipe }
	base := DialOptions{Dialer: dialer, ConnectTimeoutMs: 100}
	got := base.Override(DialOptions{})
	if got.Dialer == nil {
		t.Fatal("Override dropped a set dialer")
	}
	if got.ConnectTimeoutMs != 100 {
		t.Fatalf("ConnectTimeoutMs = %d, want 100", got.ConnectTimeoutMs)
	}
}

func TestPlainTCPDialerConnectsAndFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	conn, err := PlainTCPDialer(context.Background(), ln.Addr().String())
	if err != nil {
		t.Fatalf("dial %s: %v", ln.Addr(), err)
	}
	conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := PlainTCPDialer(ctx, "127.0.0.1:1"); err == nil {
		t.Fatal("dialing a closed port must fail")
	}
}

func TestTokenCredRefreshesFromItsSource(t *testing.T) {
	var reads int
	cred := &tokenCred{
		token: "stale",
		source: func() (string, error) {
			reads++
			return fmt.Sprintf("tok-%d", reads), nil
		},
		refreshInterval: 0,
	}
	for i := 1; i <= 2; i++ {
		md, err := cred.GetRequestMetadata(context.Background())
		if err != nil {
			t.Fatalf("GetRequestMetadata: %v", err)
		}
		want := fmt.Sprintf("%stok-%d", bearerPrefix, i)
		if got := md[authorizationHeader]; got != want {
			t.Fatalf("call %d token = %q, want %q", i, got, want)
		}
	}
}

func TestTokenCredWithoutSourceKeepsItsToken(t *testing.T) {
	cred := &tokenCred{token: "fixed", refreshInterval: 0}
	for i := 0; i < 2; i++ {
		md, err := cred.GetRequestMetadata(context.Background())
		if err != nil {
			t.Fatalf("GetRequestMetadata: %v", err)
		}
		if got := md[authorizationHeader]; got != bearerPrefix+"fixed" {
			t.Fatalf("token = %q, want the fixed token", got)
		}
	}
}

func TestTokenCredKeepsTheOldTokenWhenAReadFails(t *testing.T) {
	cred := &tokenCred{
		token:           "good",
		source:          func() (string, error) { return "", io.ErrUnexpectedEOF },
		refreshInterval: 0,
	}
	md, err := cred.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if got := md[authorizationHeader]; got != bearerPrefix+"good" {
		t.Fatalf("token = %q, want the last good token", got)
	}
}
