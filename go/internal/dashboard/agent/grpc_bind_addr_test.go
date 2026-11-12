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

package agent

import "testing"

// TestBindAddrForHost covers the address composition of the gRPC listener. The
// IPv6 row is the regression case: an unbracketed "::1:8080" is rejected by
// net.Listen, which would kill the agent at startup on a host whose localhost
// resolves to IPv6.
func TestBindAddrForHost(t *testing.T) {
	for _, tc := range []struct {
		host string
		port int
		want string
	}{
		{"127.0.0.1", 8080, "127.0.0.1:8080"},
		{"::1", 8080, "[::1]:8080"},
		{"", 8080, ":8080"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			if got := bindAddrForHost(tc.host, tc.port); got != tc.want {
				t.Fatalf("bindAddrForHost(%q, %d) = %q, want %q", tc.host, tc.port, got, tc.want)
			}
		})
	}
}
