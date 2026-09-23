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

package setup_worker

import (
	"strings"
	"testing"
)

// TestGetSetupWorkerCmd verifies the GetSetupWorkerCmd function.
func TestGetSetupWorkerCmd(t *testing.T) {
	t.Run("returns non-nil command", func(t *testing.T) {
		cmd := GetSetupWorkerCmd()
		if cmd == nil {
			t.Fatal("expected non-nil command, got nil")
		}
		if cmd.Use != "setup_worker" {
			t.Errorf("expected Use to be 'setup_worker', got %q", cmd.Use)
		}
	})
}

// TestRunSetupWorker verifies the error handling of runSetupWorker for invalid
// languages and malformed runtime env contexts.
func TestRunSetupWorker(t *testing.T) {
	testCases := []struct {
		name                        string
		serializedRuntimeEnvContext string
		language                    string
		remainingArgs               []string
		expectError                 bool
		errorContains               string
	}{
		// Invalid language test cases.
		{
			name:                        "invalid language RUSTY",
			serializedRuntimeEnvContext: "{}",
			language:                    "RUSTY",
			remainingArgs:               []string{},
			expectError:                 true,
			errorContains:               "invalid language",
		},
		{
			name:                        "invalid language JAVASCRIPT",
			serializedRuntimeEnvContext: "{}",
			language:                    "JAVASCRIPT",
			remainingArgs:               []string{},
			expectError:                 true,
			errorContains:               "invalid language",
		},
		{
			name:                        "empty language string",
			serializedRuntimeEnvContext: "{}",
			language:                    "",
			remainingArgs:               []string{},
			expectError:                 true,
			errorContains:               "--language is required",
		},
		// JSON deserialization error test cases.
		{
			name:                        "invalid JSON in serialized runtime env context",
			serializedRuntimeEnvContext: "{invalid json}",
			language:                    "PYTHON",
			remainingArgs:               []string{},
			expectError:                 true,
			errorContains:               "failed to deserialize",
		},
		{
			name:                        "malformed JSON - missing bracket",
			serializedRuntimeEnvContext: `{"command_prefix": ["echo"}`,
			language:                    "PYTHON",
			remainingArgs:               []string{},
			expectError:                 true,
			errorContains:               "failed to deserialize",
		},
		{
			name:                        "invalid JSON type for field",
			serializedRuntimeEnvContext: `{"command_prefix": "not an array"}`,
			language:                    "PYTHON",
			remainingArgs:               []string{},
			expectError:                 true,
			errorContains:               "failed to deserialize",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &SetupWorkerConfig{}

			// Pass arguments through command line flags to mimic a real CLI invocation.
			// Note: cfg cannot be assigned directly because runSetupWorker registers the
			// flags and resets the bound variables to the flag default values.
			args := []string{
				"--serialized-runtime-env-context", tc.serializedRuntimeEnvContext,
				"--language", tc.language,
			}
			args = append(args, tc.remainingArgs...)

			err := runSetupWorker(t.Context(), args, cfg)

			if tc.expectError {
				if err == nil {
					t.Errorf("expected error, got nil")
				} else if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain %q, got %q", tc.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			}
		})
	}
}
