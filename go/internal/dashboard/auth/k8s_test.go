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
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// k8sStub is a TLS stand-in for the Kubernetes API server. It records the
// Authorization header of every request so a test can observe which service
// account token the validator presented, and answers the TokenReview and
// SubjectAccessReview with an authenticated, allowed status.
type k8sStub struct {
	mu      sync.Mutex
	bearees []string
}

func (s *k8sStub) bearerTokens() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.bearees...)
}

func (s *k8sStub) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.bearees = append(s.bearees, r.Header.Get("Authorization"))
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case strings.HasSuffix(r.URL.Path, k8sTokenReviewPath):
		_, _ = w.Write([]byte(`{"status":{"authenticated":true,"user":{"username":"alice","groups":["dev"]}}}`))
	case strings.HasSuffix(r.URL.Path, k8sSubjectAccessPath):
		_, _ = w.Write([]byte(`{"status":{"allowed":true}}`))
	default:
		http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)
	}
}

// newK8sTestValidator points the package at a stub API server and a temporary
// service account directory. Everything it changes is restored when the test
// ends; it returns the validator, the service account token path, and the stub.
func newK8sTestValidator(t *testing.T) (*k8sTokenValidatorImpl, string, *k8sStub) {
	t.Helper()

	stub := &k8sStub{}
	server := httptest.NewTLSServer(http.HandlerFunc(stub.handler))
	t.Cleanup(server.Close)

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(caPath, caPEM, 0o644); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("sa-token-one\n"), 0o644); err != nil {
		t.Fatalf("write service account token: %v", err)
	}

	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse stub URL: %v", err)
	}
	t.Setenv(k8sServiceHostEnvVar, parsed.Hostname())
	t.Setenv(k8sServicePortEnvVar, parsed.Port())
	t.Setenv(rayClusterNameEnvVar, "cluster")
	t.Setenv(rayClusterNamespaceEnvVar, "ray-system")

	originalCa, originalTokenPath := k8sCaCertPath, k8sSaTokenPath
	k8sCaCertPath, k8sSaTokenPath = caPath, tokenPath
	t.Cleanup(func() { k8sCaCertPath, k8sSaTokenPath = originalCa, originalTokenPath })

	// A fresh validator per test, restored afterwards: the shared one memoizes
	// its client (and the tokens it has validated) for the process lifetime.
	originalValidator := k8sTokenValidator
	validator := &k8sTokenValidatorImpl{cache: map[string]k8sCacheEntry{}}
	k8sTokenValidator = validator
	t.Cleanup(func() { k8sTokenValidator = originalValidator })

	return validator, tokenPath, stub
}

// TestK8sValidatorPresentsRotatedServiceAccountToken proves the credential the
// validator itself uses is read per API call. Caching it in init() would keep
// presenting an expired token after kubelet rotates the projected service
// account token (about hourly), every TokenReview would answer 401, and every
// dashboard request would be rejected until the process restarted.
func TestK8sValidatorPresentsRotatedServiceAccountToken(t *testing.T) {
	validator, tokenPath, stub := newK8sTestValidator(t)
	if !validator.validate("user-token-one") {
		t.Fatal("the first token must validate against the stub")
	}

	if err := os.WriteFile(tokenPath, []byte("sa-token-two\n"), 0o644); err != nil {
		t.Fatalf("rotate service account token: %v", err)
	}
	if !validator.validate("user-token-two") {
		t.Fatal("the second token must validate against the stub")
	}

	// The stub sees two calls per validation: TokenReview then
	// SubjectAccessReview. The second pair must carry the rotated credential.
	want := []string{
		"Bearer sa-token-one", "Bearer sa-token-one",
		"Bearer sa-token-two", "Bearer sa-token-two",
	}
	got := stub.bearerTokens()
	if len(got) != len(want) {
		t.Fatalf("Authorization headers = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Authorization header %d = %q, want %q: the service account token must be re-read per call", i, got[i], want[i])
		}
	}
}

// TestK8sValidatorFailsClosedWithoutServiceAccountToken proves a missing
// service account token fails the validation instead of being sent as an empty
// bearer.
func TestK8sValidatorFailsClosedWithoutServiceAccountToken(t *testing.T) {
	validator, tokenPath, _ := newK8sTestValidator(t)
	if !validator.validate("user-token-one") {
		t.Fatal("the first token must validate against the stub")
	}
	if err := os.Remove(tokenPath); err != nil {
		t.Fatalf("remove service account token: %v", err)
	}
	if validator.validate("user-token-two") {
		t.Fatal("validation must fail when the service account token cannot be read")
	}
}
