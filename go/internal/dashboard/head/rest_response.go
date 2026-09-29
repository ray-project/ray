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
	"strings"
)

// RESTResponse writes a unified {result, msg, data} response body, aligned
// with the Python dashboard routes.py rest_response. It is encoded with
// indentation so the output matches the spacing of Python's json.dumps
// default separators (", " and ": ").
//
// It is used by the state API module whose data keys are snake_case (matching
// do_reply with convert_google_style=False). Other modules that align with the
// Python rest_response default (convert_google_style=True) should use
// RESTResponseCamel instead, which applies the same recursive key conversion.
func RESTResponse(w http.ResponseWriter, status int, msg string, data map[string]interface{}) {
	writeRESTResponse(w, status, msg, data, false)
}

// RESTResponseCamel writes a REST response whose data keys are recursively
// converted from snake_case to camelCase, matching the Python rest_response
// default of convert_google_style=True (routes.py -> to_google_style in
// dashboard/utils.py). The dashboard frontend reads camelCase keys, so the
// reporter/node/event/usage_stats/metrics/insight endpoints rely on it.
func RESTResponseCamel(w http.ResponseWriter, status int, msg string, data map[string]interface{}) {
	writeRESTResponse(w, status, msg, data, true)
}

func writeRESTResponse(w http.ResponseWriter, status int, msg string, data map[string]interface{}, toCamel bool) {
	if data == nil {
		// Python rest_response always builds data from the kwargs dict, so an
		// absent payload is an empty dict ({}), never null. Map a nil data to
		// an empty map so error responses emit "data": {} instead of "data":
		// null, aligned with the Python response shape.
		data = map[string]interface{}{}
	}
	if toCamel {
		data = toGoogleStyle(data)
	}
	body := map[string]interface{}{
		"result": status == http.StatusOK,
		"msg":    msg,
		"data":   data,
	}
	encoded, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}

// toGoogleStyle recursively converts map keys from snake_case to camelCase,
// aligned with to_google_style in python/ray/dashboard/utils.py: every dict
// key passes through to_camel_case, recursing into nested dicts and slices of
// dicts. List values of non-dict elements and scalar values are left alone.
// A nil input stays nil so data: null is preserved.
func toGoogleStyle(d map[string]interface{}) map[string]interface{} {
	if d == nil {
		return nil
	}
	out := make(map[string]interface{}, len(d))
	for k, v := range d {
		switch t := v.(type) {
		case map[string]interface{}:
			out[toCamelCase(k)] = toGoogleStyle(t)
		case []interface{}:
			newList := make([]interface{}, 0, len(t))
			for _, e := range t {
				if em, ok := e.(map[string]interface{}); ok {
					newList = append(newList, toGoogleStyle(em))
				} else {
					newList = append(newList, e)
				}
			}
			out[toCamelCase(k)] = newList
		default:
			out[toCamelCase(k)] = v
		}
	}
	return out
}

// toCamelCase converts a snake_case key to camelCase, aligned with
// to_camel_case in python/ray/dashboard/utils.py: the first component is kept
// lowercase and every following component is title-cased with the Python
// str.title() semantics (every word boundary — including letter/digit
// transitions — starts an uppercase run). This matters for keys whose
// components contain digits, e.g. the autoscaler's per-node keys
// "node_b51d27c57ced..." -> "nodeB51D27C57Ced..." (Python capitalizes the
// first letter after every digit). An empty component (from a trailing
// underscore) contributes nothing, matching str.title() on the empty string.
func toCamelCase(s string) string {
	parts := strings.Split(s, "_")
	out := parts[0]
	for _, p := range parts[1:] {
		if p == "" {
			continue
		}
		out += titleCase(p)
	}
	return out
}

// titleCase title-cases a single snake_case component with the Python
// str.title() semantics: a letter is uppercased when the previous character is
// not a letter (digit or string start), and lowercased otherwise.
func titleCase(s string) string {
	out := []byte(s)
	prevLetter := false
	for i := 0; i < len(out); i++ {
		c := out[i]
		if c >= 'A' && c <= 'Z' {
			if prevLetter {
				out[i] = c + ('a' - 'A')
			}
			prevLetter = true
			continue
		}
		if c >= 'a' && c <= 'z' {
			if !prevLetter {
				out[i] = c - ('a' - 'A')
			}
			prevLetter = true
			continue
		}
		prevLetter = false
	}
	return string(out)
}

// writeJSON writes an arbitrary JSON body with the application/json content
// type. It is used by handlers whose response shape is not the RESTResponse
// wrapper (e.g. /api/authenticate).
func writeJSON(w http.ResponseWriter, data interface{}) {
	WriteRawJSON(w, http.StatusOK, data)
}

// WriteRawJSON writes an arbitrary JSON body with a caller-chosen status code
// and the application/json content type. It is the shared writer used by the
// job/serve/data/train/flow modules whose handlers do not use the
// RESTResponse wrapper.
func WriteRawJSON(w http.ResponseWriter, status int, data interface{}) {
	encoded, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}

// ListApiResponse aligns with the Python ListApiResponse used as the body of
// state API responses. The optional fields carry no omitempty so every key is
// always present, matching asdict(ListApiResponse) in
// python/ray/util/state/common.py (which emits partial_failure_warning and
// warnings even when unset). PartialFailureWarning is a pointer so that "no
// warning" can be encoded either as the empty string (most list endpoints) or
// as null (list_objects), matching the Python dataclass default "" vs the
// explicit None passed by list_objects.
type ListApiResponse struct {
	Result                []map[string]interface{} `json:"result"`
	Total                 int                      `json:"total"`
	NumAfterTruncation    int                      `json:"num_after_truncation"`
	NumFiltered           int                      `json:"num_filtered"`
	PartialFailureWarning *string                  `json:"partial_failure_warning"`
	Warnings              *[]string                `json:"warnings"`
}
