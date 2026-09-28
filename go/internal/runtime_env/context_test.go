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
	"runtime"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/proto"
)

func TestRuntimeEnvContextSerializeContext(t *testing.T) {
	testCases := []struct {
		name         string
		ctx          RuntimeEnvContext
		expectError  bool
		expectedJSON string
	}{
		{
			name:         "empty context",
			ctx:          RuntimeEnvContext{},
			expectError:  false,
			expectedJSON: "{}",
		},
		{
			name: "context with command prefix",
			ctx: RuntimeEnvContext{
				CommandPrefix: []string{"echo", "hello"},
			},
			expectError:  false,
			expectedJSON: `{"command_prefix":["echo","hello"]}`,
		},
		{
			name: "context with env vars",
			ctx: RuntimeEnvContext{
				EnvVars: map[string]string{
					"KEY1": "value1",
					"KEY2": "value2",
				},
			},
			expectError: false,
		},
		{
			name: "context with py executable",
			ctx: RuntimeEnvContext{
				PyExecutable: "/usr/bin/python3",
			},
			expectError:  false,
			expectedJSON: `{"py_executable":"/usr/bin/python3"}`,
		},
		{
			name: "context with override worker entrypoint",
			ctx: RuntimeEnvContext{
				OverrideWorkerEntrypoint: "/app/worker.py",
			},
			expectError:  false,
			expectedJSON: `{"override_worker_entrypoint":"/app/worker.py"}`,
		},
		{
			name: "context with java jars",
			ctx: RuntimeEnvContext{
				JavaJars: []string{"/path/to/jar1.jar", "/path/to/jar2.jar"},
			},
			expectError:  false,
			expectedJSON: `{"java_jars":["/path/to/jar1.jar","/path/to/jar2.jar"]}`,
		},
		{
			name: "full context",
			ctx: RuntimeEnvContext{
				CommandPrefix:            []string{"ray"},
				EnvVars:                  map[string]string{"RAY_DEBUG": "1"},
				PyExecutable:             "/usr/bin/python3",
				OverrideWorkerEntrypoint: "/app/worker.py",
				JavaJars:                 []string{"/app/lib.jar"},
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.ctx.SerializeContext()
			if tc.expectError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
				if tc.expectedJSON != "" && result != tc.expectedJSON {
					expectedCtx, err := DeserializeContext(tc.expectedJSON)
					if err != nil {
						t.Fatalf("failed to deserialize expected JSON: %v", err)
					}
					if !compareContexts(expectedCtx, &tc.ctx) {
						t.Errorf("expected JSON %q, got %q", tc.expectedJSON, result)
					}
				}
			}
		})
	}
}

func TestDeserializeContext(t *testing.T) {
	testCases := []struct {
		name          string
		input         string
		expectError   bool
		errorContains string
		expectedCtx   RuntimeEnvContext
	}{
		{
			name:        "empty string",
			input:       "",
			expectError: false,
			expectedCtx: RuntimeEnvContext{},
		},
		{
			name:        "empty JSON object",
			input:       "{}",
			expectError: false,
			expectedCtx: RuntimeEnvContext{},
		},
		{
			name:  "valid JSON with command prefix",
			input: `{"command_prefix": ["echo", "hello"]}`,
			expectedCtx: RuntimeEnvContext{
				CommandPrefix: []string{"echo", "hello"},
			},
		},
		{
			name:  "valid JSON with env vars",
			input: `{"env_vars": {"KEY1": "value1", "KEY2": "value2"}}`,
			expectedCtx: RuntimeEnvContext{
				EnvVars: map[string]string{
					"KEY1": "value1",
					"KEY2": "value2",
				},
			},
		},
		{
			name:  "valid JSON with py executable",
			input: `{"py_executable": "/usr/bin/python3"}`,
			expectedCtx: RuntimeEnvContext{
				PyExecutable: "/usr/bin/python3",
			},
		},
		{
			name:  "valid JSON with override worker entrypoint",
			input: `{"override_worker_entrypoint": "/app/worker.py"}`,
			expectedCtx: RuntimeEnvContext{
				OverrideWorkerEntrypoint: "/app/worker.py",
			},
		},
		{
			name:  "valid JSON with java jars",
			input: `{"java_jars": ["/path/to/jar1.jar", "/path/to/jar2.jar"]}`,
			expectedCtx: RuntimeEnvContext{
				JavaJars: []string{"/path/to/jar1.jar", "/path/to/jar2.jar"},
			},
		},
		{
			name:  "valid JSON with cpp executable",
			input: `{"cpp_executable": "/usr/bin/cpp_worker"}`,
			expectedCtx: RuntimeEnvContext{
				CppExecutable: "/usr/bin/cpp_worker",
			},
		},
		{
			name:  "valid JSON with go executable",
			input: `{"go_executable": "/usr/bin/go_worker"}`,
			expectedCtx: RuntimeEnvContext{
				GoExecutable: "/usr/bin/go_worker",
			},
		},
		{
			name:  "valid JSON with all fields including cpp and go executables",
			input: `{"command_prefix": ["ray"], "env_vars": {"RAY_DEBUG": "1"}, "py_executable": "/usr/bin/python3", "override_worker_entrypoint": "/app/worker.py", "java_jars": ["/app/lib.jar"], "cpp_executable": "/usr/bin/cpp_worker", "go_executable": "/usr/bin/go_worker"}`,
			expectedCtx: RuntimeEnvContext{
				CommandPrefix:            []string{"ray"},
				EnvVars:                  map[string]string{"RAY_DEBUG": "1"},
				PyExecutable:             "/usr/bin/python3",
				OverrideWorkerEntrypoint: "/app/worker.py",
				JavaJars:                 []string{"/app/lib.jar"},
				CppExecutable:            "/usr/bin/cpp_worker",
				GoExecutable:             "/usr/bin/go_worker",
			},
		},
		{
			name:          "invalid JSON",
			input:         "{invalid json}",
			expectError:   true,
			errorContains: "failed to deserialize",
		},
		{
			name:          "malformed JSON - missing bracket",
			input:         `{"command_prefix": ["echo"}`,
			expectError:   true,
			errorContains: "failed to deserialize",
		},
		{
			name:          "invalid JSON type for field",
			input:         `{"command_prefix": "not an array"}`,
			expectError:   true,
			errorContains: "failed to deserialize",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := DeserializeContext(tc.input)
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
				if !compareContexts(&tc.expectedCtx, result) {
					t.Errorf("contexts do not match")
				}
			}
		})
	}
}

func TestRuntimeEnvContextIsEmpty(t *testing.T) {
	testCases := []struct {
		name     string
		ctx      RuntimeEnvContext
		expected bool
	}{
		{
			name:     "empty context",
			ctx:      RuntimeEnvContext{},
			expected: true,
		},
		{
			name: "context with command prefix",
			ctx: RuntimeEnvContext{
				CommandPrefix: []string{"echo"},
			},
			expected: false,
		},
		{
			name: "context with env vars",
			ctx: RuntimeEnvContext{
				EnvVars: map[string]string{"KEY": "value"},
			},
			expected: false,
		},
		{
			name: "context with py executable",
			ctx: RuntimeEnvContext{
				PyExecutable: "/usr/bin/python3",
			},
			expected: false,
		},
		{
			name: "context with override worker entrypoint",
			ctx: RuntimeEnvContext{
				OverrideWorkerEntrypoint: "/app/worker.py",
			},
			expected: false,
		},
		{
			name: "context with java jars",
			ctx: RuntimeEnvContext{
				JavaJars: []string{"/app/lib.jar"},
			},
			expected: false,
		},
		{
			name: "context with cpp executable",
			ctx: RuntimeEnvContext{
				CppExecutable: "/usr/bin/cpp_worker",
			},
			expected: false,
		},
		{
			name: "context with go executable",
			ctx: RuntimeEnvContext{
				GoExecutable: "/usr/bin/go_worker",
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.ctx.IsEmpty()
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestSerializeAndDeserializeRoundTrip(t *testing.T) {
	testCases := []struct {
		name string
		ctx  RuntimeEnvContext
	}{
		{
			name: "empty context",
			ctx:  RuntimeEnvContext{},
		},
		{
			name: "context with command prefix",
			ctx: RuntimeEnvContext{
				CommandPrefix: []string{"echo", "hello"},
			},
		},
		{
			name: "context with env vars",
			ctx: RuntimeEnvContext{
				EnvVars: map[string]string{
					"KEY1": "value1",
					"KEY2": "value2",
				},
			},
		},
		{
			name: "context with all fields",
			ctx: RuntimeEnvContext{
				CommandPrefix:            []string{"ray"},
				EnvVars:                  map[string]string{"RAY_DEBUG": "1"},
				PyExecutable:             "/usr/bin/python3",
				OverrideWorkerEntrypoint: "/app/worker.py",
				JavaJars:                 []string{"/app/lib.jar"},
				CppExecutable:            "/usr/bin/cpp_worker",
				GoExecutable:             "/usr/bin/go_worker",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			serialized, err := tc.ctx.SerializeContext()
			if err != nil {
				t.Fatalf("Serialize failed: %v", err)
			}

			deserialized, err := DeserializeContext(serialized)
			if err != nil {
				t.Fatalf("Deserialize failed: %v", err)
			}

			if !compareContexts(&tc.ctx, deserialized) {
				t.Errorf("round-trip failed: original and deserialized contexts differ")
			}
		})
	}
}

func compareContexts(a, b *RuntimeEnvContext) bool {
	if a == nil || b == nil {
		return a == b
	}
	if len(a.CommandPrefix) != len(b.CommandPrefix) {
		return false
	}
	for i, v := range a.CommandPrefix {
		if v != b.CommandPrefix[i] {
			return false
		}
	}
	if len(a.EnvVars) != len(b.EnvVars) {
		return false
	}
	for k, v := range a.EnvVars {
		if b.EnvVars[k] != v {
			return false
		}
	}
	return a.PyExecutable == b.PyExecutable &&
		a.OverrideWorkerEntrypoint == b.OverrideWorkerEntrypoint &&
		stringSliceEqual(a.JavaJars, b.JavaJars) &&
		a.CppExecutable == b.CppExecutable &&
		a.GoExecutable == b.GoExecutable
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// TestBuildWorkerCommand tests the buildWorkerCommand helper.
// buildWorkerCommand is unexported, so the test calls it directly within the package.
func TestBuildWorkerCommand(t *testing.T) {
	testCases := []struct {
		name              string
		ctx               RuntimeEnvContext
		passthroughArgs   []string
		language          proto.Language
		expectedCmdPrefix []string
		expectError       bool
		errorContains     string
	}{
		// PYTHON language.
		{
			name: "PYTHON language with py executable",
			ctx: RuntimeEnvContext{
				PyExecutable: "/usr/bin/python3",
			},
			passthroughArgs:   []string{"worker.py", "--arg1"},
			language:          proto.Language_PYTHON,
			expectedCmdPrefix: []string{"exec", "/usr/bin/python3", "worker.py", "--arg1"},
			expectError:       false,
		},
		{
			// Mirrors Python: py_executable is used as-is even when empty,
			// there is no os.Args[0] fallback.
			name:              "PYTHON language without py executable (empty, no os.Args[0] fallback)",
			ctx:               RuntimeEnvContext{},
			passthroughArgs:   []string{"worker.py"},
			language:          proto.Language_PYTHON,
			expectedCmdPrefix: []string{"exec", "", "worker.py"},
			expectError:       false,
		},
		// JAVA language.
		{
			name:            "JAVA language",
			ctx:             RuntimeEnvContext{},
			passthroughArgs: []string{"com.example.Worker"},
			language:        proto.Language_JAVA,
			expectError:     false,
		},
		{
			name: "JAVA language with java jars",
			ctx: RuntimeEnvContext{
				JavaJars: []string{"/path/to/jar1.jar", "/path/to/jar2.jar"},
			},
			passthroughArgs: []string{"com.example.Worker"},
			language:        proto.Language_JAVA,
			expectError:     false,
		},
		// CPP language.
		{
			name: "CPP language with cpp executable",
			ctx: RuntimeEnvContext{
				CppExecutable: "/usr/bin/cpp_worker",
			},
			passthroughArgs:   []string{"--arg1"},
			language:          proto.Language_CPP,
			expectedCmdPrefix: []string{"exec", "/usr/bin/cpp_worker", "--arg1"},
			expectError:       false,
		},
		{
			name:            "CPP language without cpp executable (uses os.Args[0])",
			ctx:             RuntimeEnvContext{},
			passthroughArgs: []string{"--arg1"},
			language:        proto.Language_CPP,
			expectError:     false,
		},
		// GO language.
		{
			name: "GO language with go executable",
			ctx: RuntimeEnvContext{
				GoExecutable: "/usr/bin/go_worker",
			},
			passthroughArgs:   []string{"--arg1"},
			language:          proto.Language_GO,
			expectedCmdPrefix: []string{"exec", "/usr/bin/go_worker", "--arg1"},
			expectError:       false,
		},
		{
			name:            "GO language without go executable (uses os.Args[0])",
			ctx:             RuntimeEnvContext{},
			passthroughArgs: []string{"--arg1"},
			language:        proto.Language_GO,
			expectError:     false,
		},
		// Unknown language.
		{
			name:            "unknown language",
			ctx:             RuntimeEnvContext{},
			passthroughArgs: []string{},
			language:        proto.Language(999),
			expectError:     false,
		},
		// CommandPrefix.
		{
			name: "with command prefix",
			ctx: RuntimeEnvContext{
				CommandPrefix: []string{"ray", "run"},
				PyExecutable:  "/usr/bin/python3",
			},
			passthroughArgs:   []string{"worker.py"},
			language:          proto.Language_PYTHON,
			expectedCmdPrefix: []string{"ray", "run", "exec", "/usr/bin/python3", "worker.py"},
			expectError:       false,
		},
		// OverrideWorkerEntrypoint.
		{
			name: "with override worker entrypoint",
			ctx: RuntimeEnvContext{
				OverrideWorkerEntrypoint: "/app/custom_worker.py",
				PyExecutable:             "/usr/bin/python3",
			},
			passthroughArgs:   []string{"original_worker.py", "--arg1"},
			language:          proto.Language_PYTHON,
			expectedCmdPrefix: []string{"exec", "/usr/bin/python3", "/app/custom_worker.py", "--arg1"},
			expectError:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := tc.ctx.buildWorkerCommand(tc.passthroughArgs, tc.language)

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
				if tc.expectedCmdPrefix != nil {
					// Check that the command prefix matches (accounting for the macOS DYLD_LIBRARY_PATH handling).
					if len(cmd) < len(tc.expectedCmdPrefix) {
						t.Errorf("expected cmd length >= %d, got %d", len(tc.expectedCmdPrefix), len(cmd))
					} else {
						// Compare expectedCmdPrefix and cmd from the end backwards.
						for i := range tc.expectedCmdPrefix {
							if cmd[len(cmd)-len(tc.expectedCmdPrefix)+i] != tc.expectedCmdPrefix[i] {
								t.Errorf("expected cmd[%d] to be %q, got %q. Full cmd: %v",
									len(cmd)-len(tc.expectedCmdPrefix)+i, tc.expectedCmdPrefix[i], cmd[len(cmd)-len(tc.expectedCmdPrefix)+i], cmd)
							}
						}
					}
				}
			}
		})
	}
}

// TestBuildExecPrefix tests the buildExecPrefix helper.
func TestBuildExecPrefix(t *testing.T) {
	testCases := []struct {
		name       string
		executable string
		expected   []string
	}{
		{
			name:       "with executable on Windows",
			executable: "C:\\path\\to\\python.exe",
			expected:   []string{"C:\\path\\to\\python.exe"},
		},
		{
			name:       "with executable on Unix",
			executable: "/usr/bin/python3",
			expected:   []string{"exec", "/usr/bin/python3"},
		},
		{
			name:       "empty executable on Unix",
			executable: "",
			expected:   nil, // os.Args[0] will be used.
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := (&RuntimeEnvContext{}).buildExecPrefix(tc.executable)

			if tc.expected == nil {
				// An empty executable falls back to os.Args[0].
				if len(result) == 0 {
					t.Errorf("expected non-empty result for empty executable")
				}
			} else {
				if runtime.GOOS == "windows" {
					if len(result) != 1 || result[0] != tc.expected[0] {
						t.Errorf("on Windows, expected %v, got %v", tc.expected, result)
					}
				} else {
					if len(result) != 2 || result[0] != "exec" || result[1] != tc.executable {
						t.Errorf("on Unix, expected [exec, %s], got %v", tc.executable, result)
					}
				}
			}
		})
	}
}
