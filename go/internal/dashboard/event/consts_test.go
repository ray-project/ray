// Copyright 2026 The Ray Authors.
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

package event

import (
	"os"
	"testing"
)

// TestEnvIntegerIsDigitSemantics mirrors Python's env_integer (ray_constants.py):
// only a non-empty all-digit value is accepted; negative, signed or malformed
// overrides fall back to the default instead of being honored.
func TestEnvIntegerIsDigitSemantics(t *testing.T) {
	cases := []struct {
		name string
		env  string
		set  bool
		def  int
		want int
	}{
		{name: "unset uses default", set: false, def: 42, want: 42},
		{name: "positive digits accepted", env: "7", set: true, def: 42, want: 7},
		{name: "negative rejected", env: "-5", set: true, def: 42, want: 42},
		{name: "signed plus rejected", env: "+5", set: true, def: 42, want: 42},
		{name: "non-numeric rejected", env: "abc", set: true, def: 42, want: 42},
		{name: "empty rejected", env: "", set: true, def: 42, want: 42},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const key = "EVENT_TEST_INT"
			if tc.set {
				t.Setenv(key, tc.env)
			} else if err := os.Unsetenv(key); err != nil {
				t.Fatal(err)
			}
			if got := envInteger(key, tc.def); got != tc.want {
				t.Errorf("envInteger(%q) = %d, want %d (env=%q)", key, got, tc.want, tc.env)
			}
		})
	}
}
