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

package head

import (
	"testing"
)

func TestParseConfigFromFlags(t *testing.T) {
	cfg, err := ParseConfigFromFlags([]string{
		"--host=0.0.0.0", "--port=8265", "--gcs-address=127.0.0.1:6379",
		"--cluster-id-hex=abc", "--node-ip-address=127.0.0.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPPort != 8265 {
		t.Fatalf("port = %d, want 8265 (Python-compatible default)", cfg.HTTPPort)
	}
}

func TestParseConfigFromFlagsDefaultPort(t *testing.T) {
	cfg, err := ParseConfigFromFlags([]string{"--gcs-address=127.0.0.1:6379", "--cluster-id-hex=abc", "--node-ip-address=127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPPort != 8265 {
		t.Fatalf("default port = %d, want 8265", cfg.HTTPPort)
	}
}

func TestParseConfigFromFlagsSpaceSeparated(t *testing.T) {
	cfg, err := ParseConfigFromFlags([]string{
		"--host", "0.0.0.0", "--port", "8265", "--gcs-address", "127.0.0.1:6379",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPHost != "0.0.0.0" {
		t.Fatalf("host = %q, want 0.0.0.0", cfg.HTTPHost)
	}
	if cfg.HTTPPort != 8265 {
		t.Fatalf("port = %d, want 8265 (Python-compatible default)", cfg.HTTPPort)
	}
}

func TestParseConfigFromFlagsModulesToLoad(t *testing.T) {
	cfg, err := ParseConfigFromFlags([]string{
		"--modules-to-load=StateHead, ServeHead ,", "--gcs-address=127.0.0.1:6379",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.ModulesToLoad) != 2 {
		t.Fatalf("modules = %v, want 2 entries", cfg.ModulesToLoad)
	}
	if cfg.ModulesToLoad[0] != "StateHead" || cfg.ModulesToLoad[1] != "ServeHead" {
		t.Fatalf("modules = %v, want [StateHead ServeHead]", cfg.ModulesToLoad)
	}
}

func TestParseConfigFromFlagsEmptyModulesToLoad(t *testing.T) {
	cfg, err := ParseConfigFromFlags([]string{
		"--modules-to-load=", "--gcs-address=127.0.0.1:6379",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ModulesToLoad != nil {
		t.Fatalf("modules = %v, want nil (load all)", cfg.ModulesToLoad)
	}
}

func TestSplitModules(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{"StateHead", []string{"StateHead"}},
		{"StateHead,ServeHead", []string{"StateHead", "ServeHead"}},
		{"StateHead, ServeHead ,", []string{"StateHead", "ServeHead"}},
	}
	for _, c := range cases {
		got := splitModules(c.in)
		if len(got) != len(c.want) {
			t.Fatalf("splitModules(%q) = %v, want %v", c.in, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("splitModules(%q) = %v, want %v", c.in, got, c.want)
			}
		}
	}
}

// TestDefaultHeadConfigPort is defined in http_server_test.go; the config-level
// default port assertion lives there alongside MetricsExportPort.

// TestSplitCodeSearchPath verifies splitCodeSearchPath splits on the OS
// path-list separator and drops empty entries, matching Python's
// `os.environ.get(...).split(os.pathsep)`.
func TestSplitCodeSearchPath(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{"/opt/ray", []string{"/opt/ray"}},
		{"/opt/ray:/tmp", []string{"/opt/ray", "/tmp"}},
		{"/opt/ray::/tmp", []string{"/opt/ray", "/tmp"}},
		{":/opt/ray:", []string{"/opt/ray"}},
		{"/opt/ray: /tmp", []string{"/opt/ray", "/tmp"}},
	}
	for _, c := range cases {
		got := splitCodeSearchPath(c.in)
		if len(got) != len(c.want) {
			t.Fatalf("splitCodeSearchPath(%q) = %v, want %v", c.in, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("splitCodeSearchPath(%q) = %v, want %v", c.in, got, c.want)
			}
		}
	}
}

// TestParseConfigFromFlagsCodeSearchPathFromEnv verifies the cross-language
// code_search_path is populated from RAY_DASHBOARD_CODE_SEARCH_PATH (the
// --python-source-dir flag was removed in favor of this env var, mirroring the
// Python dashboard's head.py + SubprocessModuleConfig).
func TestParseConfigFromFlagsCodeSearchPathFromEnv(t *testing.T) {
	t.Setenv("RAY_DASHBOARD_CODE_SEARCH_PATH", "/opt/ray/python:/opt/ray/extra::")
	cfg, err := ParseConfigFromFlags([]string{
		"--gcs-address=127.0.0.1:6379", "--cluster-id-hex=abc", "--node-ip-address=127.0.0.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.CodeSearchPath) != 2 {
		t.Fatalf("CodeSearchPath = %v, want 2 entries", cfg.CodeSearchPath)
	}
	if cfg.CodeSearchPath[0] != "/opt/ray/python" || cfg.CodeSearchPath[1] != "/opt/ray/extra" {
		t.Fatalf("CodeSearchPath = %v, want [/opt/ray/python /opt/ray/extra]", cfg.CodeSearchPath)
	}
}

// TestParseConfigFromFlagsCodeSearchPathUnset verifies an unset env var leaves
// CodeSearchPath nil so lazy init skips the code_search_path entirely.
func TestParseConfigFromFlagsCodeSearchPathUnset(t *testing.T) {
	t.Setenv("RAY_DASHBOARD_CODE_SEARCH_PATH", "")
	cfg, err := ParseConfigFromFlags([]string{
		"--gcs-address=127.0.0.1:6379", "--cluster-id-hex=abc", "--node-ip-address=127.0.0.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CodeSearchPath != nil {
		t.Fatalf("CodeSearchPath = %v, want nil", cfg.CodeSearchPath)
	}
}
