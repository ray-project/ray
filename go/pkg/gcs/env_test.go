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
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
)

func unsetAll() {
	for _, k := range []string{
		envUseTLS, envTLSCertFile, envTLSKeyFile, envTLSCAFile,
		envAuthMode, envAuthToken, envAuthTokenPath, envEnableK8sTokenAuth,
	} {
		os.Unsetenv(k)
	}
}

// useServiceAccountTokenFixture points the service-account token read at a
// fixture holding content, so the RAY_ENABLE_K8S_TOKEN_AUTH branch is testable
// without the /var/run/secrets/ray.io mount. The production path is restored
// when the test ends.
func useServiceAccountTokenFixture(t *testing.T, content string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ray-sa-token")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write service-account token fixture: %v", err)
	}
	prev := rayServiceAccountTokenPath
	rayServiceAccountTokenPath = path
	t.Cleanup(func() { rayServiceAccountTokenPath = prev })
}

func TestDialOptionsFromEnvPlaintext(t *testing.T) {
	unsetAll()
	defer unsetAll()
	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("DialOptionsFromEnv() error = %v, want nil", err)
	}
	if opts.CAFile != "" || opts.CertFile != "" || opts.KeyFile != "" || opts.Token != "" {
		t.Fatalf("DialOptionsFromEnv() = %+v, want all-empty (plaintext)", opts)
	}
}

func TestDialOptionsFromEnvTLSEnabled(t *testing.T) {
	unsetAll()
	defer unsetAll()
	dir := t.TempDir()
	caPath, _ := writeTestKeyPair(t, dir, "ca")
	certPath, keyPath := writeTestKeyPair(t, dir, "client")
	t.Setenv(envUseTLS, "1")
	t.Setenv(envTLSCAFile, caPath)
	t.Setenv(envTLSCertFile, certPath)
	t.Setenv(envTLSKeyFile, keyPath)

	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("DialOptionsFromEnv() error = %v, want nil", err)
	}
	if opts.CAFile != caPath || opts.CertFile != certPath || opts.KeyFile != keyPath {
		t.Fatalf("DialOptionsFromEnv() = %+v, want CA=%s cert=%s key=%s", opts, caPath, certPath, keyPath)
	}
}

func TestDialOptionsFromEnvTLSTrueCaseInsensitive(t *testing.T) {
	unsetAll()
	defer unsetAll()
	dir := t.TempDir()
	caPath, _ := writeTestKeyPair(t, dir, "ca")
	certPath, keyPath := writeTestKeyPair(t, dir, "client")
	t.Setenv(envUseTLS, "TRUE")
	t.Setenv(envTLSCAFile, caPath)
	t.Setenv(envTLSCertFile, certPath)
	t.Setenv(envTLSKeyFile, keyPath)

	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("DialOptionsFromEnv() error = %v, want nil", err)
	}
	if opts.CAFile != caPath {
		t.Fatalf("DialOptionsFromEnv() CAFile = %q, want %q", opts.CAFile, caPath)
	}
}

func TestDialOptionsFromEnvTLSMissingCerts(t *testing.T) {
	unsetAll()
	defer unsetAll()
	t.Setenv(envUseTLS, "1")
	if _, err := DialOptionsFromEnv(); err == nil {
		t.Fatal("DialOptionsFromEnv() = nil error, want error when TLS certs missing")
	}
}

// TestK8sAuthModeValueIsNotAccepted pins Ray's two-state authentication
// model: RAY_AUTH_MODE is "token" or off, so "k8s" attaches nothing even when
// a service-account token is available and the RAY_ENABLE_K8S_TOKEN_AUTH flag
// is on.
func TestK8sAuthModeValueIsNotAccepted(t *testing.T) {
	unsetAll()
	defer unsetAll()
	t.Setenv("HOME", t.TempDir())
	useServiceAccountTokenFixture(t, "ray-sa-token\n")

	t.Setenv("RAY_AUTH_MODE", "k8s")
	t.Setenv("RAY_ENABLE_K8S_TOKEN_AUTH", "1")
	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("DialOptionsFromEnv: %v", err)
	}
	if opts.Token != "" {
		t.Fatalf("token = %q, want none: k8s is not an accepted mode", opts.Token)
	}
}

func TestK8sTokenAuthFlagAttachesTheRayServiceAccountToken(t *testing.T) {
	unsetAll()
	defer unsetAll()
	// The service-account token lives at the /var/run/secrets/ray.io mount in a
	// pod; point the read at a fixture. HOME is isolated so the
	// ~/.ray/auth_token fallback cannot supply the token by accident.
	t.Setenv("HOME", t.TempDir())
	useServiceAccountTokenFixture(t, "ray-sa-token\n")

	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_ENABLE_K8S_TOKEN_AUTH", "true")
	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("DialOptionsFromEnv: %v", err)
	}
	if opts.Token == "" {
		t.Fatal("expected the ray.io service-account token under the flag")
	}
}

func TestMissingTokenDegradesGracefully(t *testing.T) {
	unsetAll()
	defer unsetAll()
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("HOME", t.TempDir()) // no ~/.ray/auth_token, no RAY_AUTH_TOKEN(_PATH)
	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("missing token must not error: %v", err)
	}
	if opts.Token != "" {
		t.Fatalf("token = %q, want empty", opts.Token)
	}
}

func TestTokenFileSkipsABlankFirstLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("\nabc\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	tok, err := ReadTokenFile(path)
	if err != nil {
		t.Fatalf("ReadTokenFile: %v", err)
	}
	if tok != "abc" {
		t.Fatalf("token = %q, want abc (first non-empty line)", tok)
	}
}

func TestDialOptionsFromEnvTokenFromEnvVar(t *testing.T) {
	unsetAll()
	defer unsetAll()
	t.Setenv(envAuthMode, "token")
	t.Setenv(envAuthToken, "secret-token")

	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("DialOptionsFromEnv() error = %v, want nil", err)
	}
	if opts.Token != "secret-token" {
		t.Fatalf("DialOptionsFromEnv() Token = %q, want %q", opts.Token, "secret-token")
	}
}

func TestDialOptionsFromEnvTokenFromFile(t *testing.T) {
	unsetAll()
	defer unsetAll()
	tokenPath := filepath.Join(t.TempDir(), "auth_token")
	if err := os.WriteFile(tokenPath, []byte("  file-token\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	t.Setenv(envAuthMode, "token")
	t.Setenv(envAuthTokenPath, tokenPath)

	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("DialOptionsFromEnv() error = %v, want nil", err)
	}
	if opts.Token != "file-token" {
		t.Fatalf("DialOptionsFromEnv() Token = %q, want %q", opts.Token, "file-token")
	}
}

func TestDialOptionsFromEnvTokenEnvVarTakesPrecedence(t *testing.T) {
	unsetAll()
	defer unsetAll()
	tokenPath := filepath.Join(t.TempDir(), "auth_token")
	if err := os.WriteFile(tokenPath, []byte("file-token\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	t.Setenv(envAuthMode, "token")
	t.Setenv(envAuthToken, "env-token")
	t.Setenv(envAuthTokenPath, tokenPath)

	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("DialOptionsFromEnv() error = %v, want nil", err)
	}
	if opts.Token != "env-token" {
		t.Fatalf("DialOptionsFromEnv() Token = %q, want env-token (env var precedence)", opts.Token)
	}
}

func TestDialOptionsFromEnvTokenDefaultPath(t *testing.T) {
	unsetAll()
	defer unsetAll()
	home := t.TempDir()
	tokenDir := filepath.Join(home, ".ray")
	if err := os.MkdirAll(tokenDir, 0o700); err != nil {
		t.Fatalf("mkdir ~/.ray: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tokenDir, "auth_token"), []byte("home-token\n"), 0o600); err != nil {
		t.Fatalf("write ~/.ray/auth_token: %v", err)
	}
	t.Setenv(envAuthMode, "token")
	t.Setenv("HOME", home)

	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("DialOptionsFromEnv() error = %v, want nil", err)
	}
	if opts.Token != "home-token" {
		t.Fatalf("DialOptionsFromEnv() Token = %q, want %q (default ~/.ray/auth_token)", opts.Token, "home-token")
	}
}

// TestDefaultAuthTokenPathFallsBackToTheUserDatabase pins the C++
// GetDefaultTokenPath alignment: with HOME unset, the default token path falls
// back to the user database instead of resolving nothing. The test host has a
// passwd entry, so user.Current() supplies the fallback home.
func TestDefaultAuthTokenPathFallsBackToTheUserDatabase(t *testing.T) {
	t.Setenv("HOME", "")
	u, err := user.Current()
	if err != nil {
		t.Fatalf("user.Current() failed; the HOME fallback needs the user database: %v", err)
	}
	if u.HomeDir == "" {
		t.Fatal("user.Current().HomeDir is empty; the fallback cannot be exercised on this host")
	}
	want := filepath.Join(u.HomeDir, DefaultAuthTokenPath)
	got := defaultAuthTokenPath()
	if got != want {
		t.Fatalf("defaultAuthTokenPath() = %q, want %q (user-database fallback)", got, want)
	}
	if !strings.Contains(got, u.HomeDir) {
		t.Fatalf("defaultAuthTokenPath() = %q, want it to contain the fallback home %q", got, u.HomeDir)
	}
}

func TestDialOptionsFromEnvTokenPathBeatsDefault(t *testing.T) {
	unsetAll()
	defer unsetAll()
	home := t.TempDir()
	tokenDir := filepath.Join(home, ".ray")
	if err := os.MkdirAll(tokenDir, 0o700); err != nil {
		t.Fatalf("mkdir ~/.ray: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tokenDir, "auth_token"), []byte("home-token\n"), 0o600); err != nil {
		t.Fatalf("write ~/.ray/auth_token: %v", err)
	}
	tokenPath := filepath.Join(t.TempDir(), "auth_token")
	if err := os.WriteFile(tokenPath, []byte("path-token\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	t.Setenv(envAuthMode, "token")
	t.Setenv(envAuthTokenPath, tokenPath)
	t.Setenv("HOME", home)

	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("DialOptionsFromEnv() error = %v, want nil", err)
	}
	if opts.Token != "path-token" {
		t.Fatalf("DialOptionsFromEnv() Token = %q, want path-token (token path beats default)", opts.Token)
	}
}

func TestDialOptionsFromEnvAuthModeDisabled(t *testing.T) {
	unsetAll()
	defer unsetAll()
	t.Setenv(envAuthMode, "disabled")
	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("DialOptionsFromEnv() error = %v, want nil", err)
	}
	if opts.Token != "" {
		t.Fatalf("DialOptionsFromEnv() Token = %q, want empty when auth disabled", opts.Token)
	}
}

func TestReadTokenFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "token")
	if err := os.WriteFile(p, []byte("  my-token\nsecond-line\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	got, err := ReadTokenFile(p)
	if err != nil {
		t.Fatalf("ReadTokenFile() error = %v", err)
	}
	if got != "my-token" {
		t.Fatalf("ReadTokenFile() = %q, want %q (first line, trimmed)", got, "my-token")
	}

	// Missing file surfaces the underlying error.
	if _, err := ReadTokenFile(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("ReadTokenFile(missing) = nil error, want error")
	}
}

func TestIsEnvTrue(t *testing.T) {
	unsetAll()
	defer unsetAll()
	cases := []struct {
		val  string
		want bool
	}{
		{"1", true},
		{"true", true},
		{"TRUE", true},
		{"True", true},
		{" 1 ", true},
		{"0", false},
		{"false", false},
		{"yes", false},
		{"", false},
	}
	for _, c := range cases {
		t.Setenv(envUseTLS, c.val)
		if got := isEnvTrue(envUseTLS); got != c.want {
			t.Errorf("isEnvTrue(%q) = %v, want %v", c.val, got, c.want)
		}
	}
}

func TestAuthTokenPathThatCannotBeReadIsAnError(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN_PATH", filepath.Join(t.TempDir(), "absent"))
	t.Setenv("HOME", t.TempDir())
	if _, err := DialOptionsFromEnv(); err == nil {
		t.Fatal("a RAY_AUTH_TOKEN_PATH that cannot be read must fail instead of " +
			"falling through to another token source")
	}
}

func TestAuthTokenPathThatIsEmptyIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("\n \n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN_PATH", path)
	t.Setenv("HOME", t.TempDir())
	if _, err := DialOptionsFromEnv(); err == nil {
		t.Fatal("a RAY_AUTH_TOKEN_PATH holding no token must fail")
	}
}

func TestK8sServiceAccountTokenIsRefreshable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sa-token")
	if err := os.WriteFile(path, []byte("sa-token-v1\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	prev := rayServiceAccountTokenPath
	rayServiceAccountTokenPath = path
	t.Cleanup(func() { rayServiceAccountTokenPath = prev })

	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_ENABLE_K8S_TOKEN_AUTH", "1")
	t.Setenv("HOME", t.TempDir())

	opts, err := DialOptionsFromEnv()
	if err != nil {
		t.Fatalf("DialOptionsFromEnv: %v", err)
	}
	if opts.TokenSource == nil {
		t.Fatal("a service-account token rotates; the options must carry a source")
	}
	if err := os.WriteFile(path, []byte("sa-token-v2\n"), 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	fresh, err := opts.TokenSource()
	if err != nil {
		t.Fatalf("TokenSource: %v", err)
	}
	if fresh != "sa-token-v2" {
		t.Fatalf("TokenSource = %q, want the rotated token sa-token-v2", fresh)
	}
}

func TestFixedTokenSourcesCarryNoRefreshFunction(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "fixed-token")
	if opts, err := DialOptionsFromEnv(); err != nil || opts.TokenSource != nil {
		t.Fatalf("opts = %+v, err = %v; a fixed env token needs no refresh", opts, err)
	}
}
