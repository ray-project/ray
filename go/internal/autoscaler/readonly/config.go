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
	_ "embed"
)

// defaultsYaml is the readonly provider's default config, embedded into the
// binary so the deployed raygo executable does not depend on the source tree,
// runfiles, or wheel file layout.
//
//go:embed defaults.yaml
var defaultsYaml []byte

// LoadReadonlyDefaultsConfig returns the readonly provider's default config content.
func LoadReadonlyDefaultsConfig() ([]byte, error) {
	return defaultsYaml, nil
}
