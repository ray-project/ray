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

package agent_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/internal/dashboard/log"
)

// TestRouterServesLogModule verifies the real aiohttp router serves the log
// module: /logs renders the directory index and /logs/<file> is served as a
// file. A router that treats every trailing-slash pattern as a redirect would
// fail both, because the module registers the /logs/ subtree.
func TestRouterServesLogModule(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.log"), []byte("line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mod, err := log.New(agent.Config{LogDir: dir})
	if err != nil {
		t.Fatalf("log.New: %v", err)
	}
	h, err := agent.NewRouterForTest([]agent.Module{mod})
	if err != nil {
		t.Fatalf("NewRouterForTest: %v", err)
	}

	for _, tc := range []struct{ target, body string }{
		{"http://agent/logs", "a.log"},
		{"http://agent/logs/a.log", "line\n"},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.target, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", tc.target, rec.Code)
			continue
		}
		if !strings.Contains(rec.Body.String(), tc.body) {
			t.Errorf("GET %s body = %q, want it to contain %q", tc.target, rec.Body.String(), tc.body)
		}
	}
}
