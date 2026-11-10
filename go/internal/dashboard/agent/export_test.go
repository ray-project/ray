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

package agent

import "net/http"

// NewRouterForTest exposes the aiohttp-compatible router to the external test
// package. It lets a test register real modules (for example the log module,
// which cannot be imported from the internal test package without an import
// cycle) and exercise their routes through the handler the agent serves.
func NewRouterForTest(mods []Module) (http.Handler, error) {
	mux := http.NewServeMux()
	for _, m := range mods {
		if err := m.RegisterHTTP(mux); err != nil {
			return nil, err
		}
	}
	return &aiohttpRouter{mux: mux}, nil
}
