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

package readonly

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLoadReadonlyDefaultsConfig tests loading the embedded readonly default config.
func TestLoadReadonlyDefaultsConfig(t *testing.T) {
	content, err := LoadReadonlyDefaultsConfig()

	assert.NoError(t, err)
	assert.NotEmpty(t, content)

	// The embedded config must keep the readonly provider type.
	assert.Contains(t, string(content), "type: readonly")
}

// BenchmarkLoadReadonlyDefaultsConfig is the benchmark.
func BenchmarkLoadReadonlyDefaultsConfig(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = LoadReadonlyDefaultsConfig()
	}
}
