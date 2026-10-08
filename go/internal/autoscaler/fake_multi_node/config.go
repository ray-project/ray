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
	_ "embed"
)

// exampleYaml is the fake_multinode provider's default config, embedded into
// the binary so the deployed raygo executable does not depend on the source
// tree, runfiles, or wheel file layout.
//
//go:embed example.yaml
var exampleYaml []byte

// exampleDockerYaml is the fake_multinode_docker provider's default config.
//
//go:embed example_docker.yaml
var exampleDockerYaml []byte

// LoadFakeMultinodeDefaultsConfig returns the fake_multinode default config content.
func LoadFakeMultinodeDefaultsConfig() ([]byte, error) {
	return exampleYaml, nil
}

// LoadFakeMultinodeDockerDefaultsConfig returns the fake_multinode_docker default config content.
func LoadFakeMultinodeDockerDefaultsConfig() ([]byte, error) {
	return exampleDockerYaml, nil
}
