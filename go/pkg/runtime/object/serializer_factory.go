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

package object

// defaultSerializer is the global serializer instance.
// It is set during package initialization by the serializer package.
// This allows high-level packages to use serialization without
// directly depending on the serializer implementation.
var defaultSerializer Serializer

// GetSerializer returns the default Serializer implementation.
// This function is used by high-level packages (e.g., pkg/runtime/api)
// to obtain a serializer for serializing/deserializing objects.
//
// The actual implementation is registered by the serializer package
// during its init() function, following the Dependency Inversion Principle.
//
// Returns:
//   - Serializer: the default serializer implementation
//
// Note: This function will panic if called before the serializer package
// has been initialized. This should not happen in normal usage because
// the serializer package is imported by the runtime initialization code.
func GetSerializer() Serializer {
	if defaultSerializer == nil {
		// This should not happen in normal usage.
		// The serializer package should have registered itself during init().
		panic("serializer not initialized - this is a bug in the runtime initialization")
	}
	return defaultSerializer
}

// SetSerializer sets the default Serializer implementation.
// This function should only be called by the serializer package
// during its init() function.
//
// Parameters:
//   - s: the serializer implementation to use as default
//
// Warning: This function is not thread-safe and should only be called
// once during package initialization. Calling it multiple times or
// concurrently can lead to race conditions and unpredictable behavior.
func SetSerializer(s Serializer) {
	defaultSerializer = s
}
