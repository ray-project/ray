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

package head

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestToGoogleStyle pins the recursive snake_case -> camelCase conversion
// against to_google_style in python/ray/dashboard/utils.py.
func TestToGoogleStyle(t *testing.T) {
	in := map[string]interface{}{
		"cluster_status": "ok",
		"autoscaling_":   "trailing",
		"node_id":        "abc",
		"nested":         map[string]interface{}{"inner_key": 1},
		"items":          []interface{}{map[string]interface{}{"item_id": "x"}, "scalar"},
		"already_camel":  "kept",
	}
	out := toGoogleStyle(in)
	if out["clusterStatus"] != "ok" {
		t.Fatalf("clusterStatus = %v", out["clusterStatus"])
	}
	if out["nodeId"] != "abc" {
		t.Fatalf("nodeId = %v", out["nodeId"])
	}
	if out["alreadyCamel"] != "kept" {
		t.Fatalf("alreadyCamel = %v", out["alreadyCamel"])
	}
	nested, ok := out["nested"].(map[string]interface{})
	if !ok || nested["innerKey"] != 1 {
		t.Fatalf("nested = %v", out["nested"])
	}
	items, ok := out["items"].([]interface{})
	if !ok || len(items) != 2 {
		t.Fatalf("items = %v", out["items"])
	}
	first, ok := items[0].(map[string]interface{})
	if !ok || first["itemId"] != "x" {
		t.Fatalf("items[0] = %v", items[0])
	}
	if items[1] != "scalar" {
		t.Fatalf("items[1] = %v", items[1])
	}
	// Trailing underscore yields an empty component which contributes nothing.
	if out["autoscaling"] != "trailing" {
		t.Fatalf("autoscaling = %v", out["autoscaling"])
	}
}

// TestToGoogleStyleNil preserves data: null for nil payloads.
func TestToGoogleStyleNil(t *testing.T) {
	if got := toGoogleStyle(nil); got != nil {
		t.Fatalf("toGoogleStyle(nil) = %v, want nil", got)
	}
}

// TestRESTResponseNilDataIsEmptyObject pins the nil-data fallback: an error
// response with no payload must encode "data": {} (empty object), not "data":
// null, aligned with the Python rest_response which always builds data from
// the kwargs dict. This covers e.g. /nodes?view=unknown (Go passed nil where
// Python rest_response(INTERNAL_ERROR, ...) emits data: {}).
func TestRESTResponseNilDataIsEmptyObject(t *testing.T) {
	rec := httptest.NewRecorder()
	RESTResponseCamel(rec, http.StatusInternalServerError, "Unknown view foo", nil)
	body := strings.TrimSpace(rec.Body.String())
	var parsed struct {
		Data map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("invalid JSON body %s: %v", body, err)
	}
	if parsed.Data == nil {
		t.Fatalf("data must be {} not null, body = %s", body)
	}
	if strings.Contains(body, `"data": null`) {
		t.Fatalf("body must not contain data: null, got %s", body)
	}
}

// TestToCamelCase pins individual conversions.
func TestToCamelCase(t *testing.T) {
	cases := map[string]string{
		"cluster_status":    "clusterStatus",
		"autoscaling_error": "autoscalingError",
		"nodeId":            "nodeId",
		"a_b_c":             "aBC",
		"single":            "single",
	}
	for in, want := range cases {
		if got := toCamelCase(in); got != want {
			t.Fatalf("toCamelCase(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestToCamelCaseTitleSemantics pins the Python str.title() semantics: word
// boundaries include letter/digit transitions, so the autoscaler per-node keys
// ("node_b51d27c57ced...") are capitalized after every digit, matching
// to_camel_case("node_b51d27c57ced...") in python/ray/dashboard/utils.py.
func TestToCamelCaseTitleSemantics(t *testing.T) {
	got := toCamelCase("node_b51d27c57ced320017f70aba9224a2124bcb47ae3ed8827b5602ee7e")
	want := "nodeB51D27C57Ced320017F70Aba9224A2124Bcb47Ae3Ed8827B5602Ee7E"
	if got != want {
		t.Fatalf("toCamelCase(hex key) = %q, want %q", got, want)
	}
	// A trailing underscore contributes nothing (empty title component).
	if got := toCamelCase("autoscaling_"); got != "autoscaling" {
		t.Fatalf("toCamelCase(autoscaling_) = %q, want autoscaling", got)
	}
}
