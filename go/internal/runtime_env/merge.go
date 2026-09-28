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

package runtime_env

import (
	"github.com/mohae/deepcopy"
)

// MergeRuntimeEnv merges two RuntimeEnv values.
// If override is true, fields from overrideEnv take precedence over fields in baseEnv.
// Otherwise, a field is only added when it is absent from baseEnv.
// Special handling:
// - env_vars: the two maps are merged, and a conflict is reported only when the same key has different values.
// - Returns nil when a conflict exists and override is false.
func MergeRuntimeEnv(baseEnv, overrideEnv *RuntimeEnv, override bool) *RuntimeEnv {
	if baseEnv == nil && overrideEnv == nil {
		result := &RuntimeEnv{}
		*result = make(RuntimeEnv)
		return result
	}

	if baseEnv == nil {
		baseEnv = &RuntimeEnv{}
		*baseEnv = make(RuntimeEnv)
	}

	if overrideEnv == nil {
		return baseEnv
	}

	// Deep copy baseEnv so the original data is not modified.
	result := &RuntimeEnv{}
	*result = make(RuntimeEnv)
	for k, v := range *baseEnv {
		(*result)[k] = deepcopy.Copy(v)
	}

	// Merge the fields from overrideEnv.
	hasConflict := false
	for k, v := range *overrideEnv {
		if existingVal, exists := (*result)[k]; exists {
			// Special handling for the env_vars field.
			if k == FieldEnvVars {
				// Merge the env_vars maps.
				if baseMap, ok := existingVal.(map[string]string); ok {
					if overrideMap, ok2 := v.(map[string]string); ok2 {
						// Check for conflicting keys.
						hasKeyConflict := false
						for key, val := range overrideMap {
							if baseVal, exists := baseMap[key]; exists && baseVal != val {
								hasKeyConflict = true
								break
							}
						}
						if hasKeyConflict && !override {
							hasConflict = true
							break
						}
						// Merge the maps.
						mergedMap := make(map[string]string)
						for key, val := range baseMap {
							mergedMap[key] = val
						}
						for key, val := range overrideMap {
							mergedMap[key] = val
						}
						(*result)[k] = mergedMap
						continue
					}
				}
			}

			// Other fields: an existing value counts as a conflict when override is false.
			if !override {
				hasConflict = true
				break
			}
		}
		(*result)[k] = deepcopy.Copy(v)
	}

	if hasConflict {
		return nil
	}

	return result
}
