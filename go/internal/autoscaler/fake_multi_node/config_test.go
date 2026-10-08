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

package fake_multi_node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLoadFakeMultinodeDefaultsConfig tests loading the embedded fake_multinode default config.
func TestLoadFakeMultinodeDefaultsConfig(t *testing.T) {
	content, err := LoadFakeMultinodeDefaultsConfig()

	assert.NoError(t, err)
	assert.NotEmpty(t, content)

	// The embedded config must keep the fake_multinode provider type.
	assert.Contains(t, string(content), "type: fake_multinode")
}

// TestLoadFakeMultinodeDockerDefaultsConfig tests loading the embedded fake_multinode_docker default config.
func TestLoadFakeMultinodeDockerDefaultsConfig(t *testing.T) {
	content, err := LoadFakeMultinodeDockerDefaultsConfig()

	assert.NoError(t, err)
	assert.NotEmpty(t, content)

	// The embedded config must keep the fake_multinode_docker provider type.
	assert.Contains(t, string(content), "type: fake_multinode_docker")
}

// BenchmarkLoadFakeMultinodeDefaultsConfig is the benchmark.
func BenchmarkLoadFakeMultinodeDefaultsConfig(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = LoadFakeMultinodeDefaultsConfig()
	}
}

// BenchmarkLoadFakeMultinodeDockerDefaultsConfig is the benchmark.
func BenchmarkLoadFakeMultinodeDockerDefaultsConfig(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = LoadFakeMultinodeDockerDefaultsConfig()
	}
}
