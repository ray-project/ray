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

package auth

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
)

// clearTokenEnv isolates a test from the ambient token environment and points
// HOME at an empty directory so the default token path cannot leak in.
func clearTokenEnv(t *testing.T) {
	t.Helper()
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv(envAuthToken, "")
	t.Setenv(envAuthTokenPath, "")
	t.Setenv(envEnableK8sTokenAuth, "")
	t.Setenv("HOME", t.TempDir())
}

// TestRequestTokenDisabled checks that no token is returned when token
// authentication is off, even when every source is populated.
func TestRequestTokenDisabled(t *testing.T) {
	clearTokenEnv(t)
	t.Setenv(envAuthToken, "secret-token")
	for _, mode := range []string{"", "disabled"} {
		t.Setenv("RAY_AUTH_MODE", mode)
		if token, ok := RequestToken(); ok {
			t.Errorf("mode %q: RequestToken = (%q, true), want no token", mode, token)
		}
	}
}

// TestRequestTokenFromEnvTrimmed checks precedence 1 and the whitespace trim:
// a trailing newline in RAY_AUTH_TOKEN must not reach the header.
func TestRequestTokenFromEnvTrimmed(t *testing.T) {
	clearTokenEnv(t)
	t.Setenv(envAuthToken, "secret-token\n")
	if token, ok := RequestToken(); !ok || token != "secret-token" {
		t.Errorf("RequestToken = (%q, %v), want (secret-token, true)", token, ok)
	}
}

// TestRequestTokenFromPathFile checks precedence 2: the first non-empty line of
// the RAY_AUTH_TOKEN_PATH file, trimmed.
func TestRequestTokenFromPathFile(t *testing.T) {
	clearTokenEnv(t)
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("\n  file-token  \nsecond\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envAuthTokenPath, path)
	if token, ok := RequestToken(); !ok || token != "file-token" {
		t.Errorf("RequestToken = (%q, %v), want (file-token, true)", token, ok)
	}
}

// TestRequestTokenPathUnreadableIsHardStop checks the C++ error semantics: a
// RAY_AUTH_TOKEN_PATH that is set but unreadable stops the lookup instead of
// falling through to the default path.
func TestRequestTokenPathUnreadableIsHardStop(t *testing.T) {
	clearTokenEnv(t)
	t.Setenv(envAuthTokenPath, filepath.Join(t.TempDir(), "missing"))
	// The fixture must live where the accessor looks (the HOME set by
	// clearTokenEnv), otherwise the test would pass even without the hard stop.
	writeDefaultToken(t, os.Getenv("HOME"), "default-token\n")
	if token, ok := RequestToken(); ok {
		t.Errorf("RequestToken = (%q, true), want no token (the broken path is a hard stop)", token)
	}
}

// TestRequestTokenWhitespaceOnlyEnvIsHardStop checks the C++ trim-and-stop
// semantics: a whitespace-only RAY_AUTH_TOKEN trims to empty and stops the
// lookup instead of falling through to the default path.
func TestRequestTokenWhitespaceOnlyEnvIsHardStop(t *testing.T) {
	clearTokenEnv(t)
	t.Setenv(envAuthToken, " \t\n ")
	writeDefaultToken(t, os.Getenv("HOME"), "default-token\n")
	if token, ok := RequestToken(); ok {
		t.Errorf("RequestToken = (%q, true), want no token (a whitespace-only env is a hard stop)", token)
	}
}

// TestRequestTokenFromDefaultPath checks precedence 4: when all environment
// sources are empty the token comes from $HOME/.ray/auth_token.
func TestRequestTokenFromDefaultPath(t *testing.T) {
	clearTokenEnv(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	writeDefaultToken(t, home, "default-token\n")
	if token, ok := RequestToken(); !ok || token != "default-token" {
		t.Errorf("RequestToken = (%q, %v), want (default-token, true)", token, ok)
	}
}

// TestRequestTokenFromK8sServiceAccount checks precedence 3: with
// RAY_ENABLE_K8S_TOKEN_AUTH on and no environment/file source, the Ray
// service-account token is used.
func TestRequestTokenFromK8sServiceAccount(t *testing.T) {
	clearTokenEnv(t)
	t.Setenv(envEnableK8sTokenAuth, "1")
	path := filepath.Join(t.TempDir(), "sa-token")
	if err := os.WriteFile(path, []byte("sa-token\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	oldPath := RayServiceAccountTokenPath
	RayServiceAccountTokenPath = path
	t.Cleanup(func() { RayServiceAccountTokenPath = oldPath })

	if token, ok := RequestToken(); !ok || token != "sa-token" {
		t.Errorf("RequestToken = (%q, %v), want (sa-token, true)", token, ok)
	}
}

// TestRequestTokenPrecedence checks the ordering: an environment token wins
// over every file source, and a token path file wins over the k8s service
// account token.
func TestRequestTokenPrecedence(t *testing.T) {
	clearTokenEnv(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	writeDefaultToken(t, home, "default-token\n")

	saPath := filepath.Join(t.TempDir(), "sa-token")
	if err := os.WriteFile(saPath, []byte("sa-token\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	oldPath := RayServiceAccountTokenPath
	RayServiceAccountTokenPath = saPath
	t.Cleanup(func() { RayServiceAccountTokenPath = oldPath })
	t.Setenv(envEnableK8sTokenAuth, "1")

	if token, _ := RequestToken(); token != "sa-token" {
		t.Fatalf("RequestToken = %q, want the service-account token before the default path", token)
	}

	pathFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(pathFile, []byte("path-token\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envAuthTokenPath, pathFile)
	if token, _ := RequestToken(); token != "path-token" {
		t.Fatalf("RequestToken = %q, want the RAY_AUTH_TOKEN_PATH token before the service-account token", token)
	}

	t.Setenv(envAuthToken, "env-token\n")
	if token, _ := RequestToken(); token != "env-token" {
		t.Fatalf("RequestToken = %q, want the RAY_AUTH_TOKEN token first", token)
	}
}

// TestExpectedTokenTrimsEnv pins the inbound half of the trim. The expected
// value is compared against the bearer a client sends, and RequestToken trims
// the same environment value before attaching it; without the trim here a
// RAY_AUTH_TOKEN carrying a trailing newline would make every token-mode
// request against a Go dashboard fail the comparison.
func TestExpectedTokenTrimsEnv(t *testing.T) {
	clearTokenEnv(t)
	t.Setenv(envAuthToken, "  secret-token\n")
	if got := expectedToken(); got != "secret-token" {
		t.Fatalf("expectedToken = %q, want the trimmed value", got)
	}
	if !ValidateRequestToken("Bearer secret-token") {
		t.Fatal("a request carrying the trimmed token must be accepted")
	}
}

// TestExpectedTokenEmptyEnvFallsThroughToPath pins the set-but-empty case: an
// empty RAY_AUTH_TOKEN is not authoritative, matching RequestToken, so the
// RAY_AUTH_TOKEN_PATH file still supplies the token.
func TestExpectedTokenEmptyEnvFallsThroughToPath(t *testing.T) {
	clearTokenEnv(t)
	pathFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(pathFile, []byte("path-token\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envAuthTokenPath, pathFile)
	t.Setenv(envAuthToken, "")
	if got := expectedToken(); got != "path-token" {
		t.Fatalf("expectedToken = %q, want the file's token", got)
	}
}

// TestExpectedTokenRereadsRotatedFile proves the expected token follows a
// rotated file: nothing about the value is cached, so a rotated token takes
// effect without a restart.
func TestExpectedTokenRereadsRotatedFile(t *testing.T) {
	clearTokenEnv(t)
	pathFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(pathFile, []byte("first-token\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envAuthTokenPath, pathFile)
	if got := expectedToken(); got != "first-token" {
		t.Fatalf("expectedToken = %q, want first-token", got)
	}

	if err := os.WriteFile(pathFile, []byte("second-token\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := expectedToken(); got != "second-token" {
		t.Fatalf("expectedToken = %q, want the rotated token", got)
	}
	if ValidateRequestToken("Bearer first-token") {
		t.Fatal("the superseded token must no longer be accepted")
	}
}

// writeDefaultToken writes content to <home>/.ray/auth_token.
func writeDefaultToken(t *testing.T, home, content string) {
	t.Helper()
	path := filepath.Join(home, gcs.DefaultAuthTokenPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
