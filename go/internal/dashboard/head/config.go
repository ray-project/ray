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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ParseConfigFromFlags parses the dashboard CLI arguments (aligned with the
// dashboard.py argparse). Both "--flag value" and "--flag=value" forms are
// accepted. The --port value is used directly (Python compatible default 8265).
func ParseConfigFromFlags(args []string) (*HeadConfig, error) {
	cfg := DefaultHeadConfig()
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name := arg
		value := ""
		hasValue := false
		if eq := strings.IndexByte(arg, '='); eq >= 0 {
			name = arg[:eq]
			value = arg[eq+1:]
			hasValue = true
		}
		// next returns the value following the flag, consuming it from args.
		next := func() (string, error) {
			if hasValue {
				return value, nil
			}
			if i+1 >= len(args) {
				return "", fmt.Errorf("flag %s requires a value", name)
			}
			i++
			return args[i], nil
		}
		switch name {
		case "--host":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.HTTPHost = v
		case "--port":
			v, err := next()
			if err != nil {
				return nil, err
			}
			p, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("invalid --port: %w", err)
			}
			cfg.HTTPPort = p
		case "--port-retries":
			v, err := next()
			if err != nil {
				return nil, err
			}
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("invalid --port-retries: %w", err)
			}
			cfg.HTTPPortRetries = n
		case "--gcs-address":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.GCSAddress = v
		case "--cluster-id-hex":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.ClusterIDHex = v
		case "--node-ip-address":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.NodeIPAddress = v
		case "--session-dir":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.SessionDir = v
			cfg.SessionName = filepath.Base(v)
		case "--log-dir":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.LogDir = v
		case "--temp-dir":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.TempDir = v
		case "--logging-level":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.LoggingLevel = v
		case "--logging-format":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.LoggingFormat = v
		case "--logging-filename":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.LoggingFilename = v
		case "--logging-rotate-bytes":
			v, err := next()
			if err != nil {
				return nil, err
			}
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("invalid --logging-rotate-bytes: %w", err)
			}
			cfg.LoggingRotateBytes = n
		case "--logging-rotate-backup-count":
			v, err := next()
			if err != nil {
				return nil, err
			}
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("invalid --logging-rotate-backup-count: %w", err)
			}
			cfg.LoggingRotateBackup = n
		case "--modules-to-load":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.ModulesToLoad = splitModules(v)
		case "--stdout-filepath":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.StdoutFilepath = v
		case "--stderr-filepath":
			v, err := next()
			if err != nil {
				return nil, err
			}
			cfg.StderrFilepath = v
		case "--minimal":
			cfg.Minimal = true
			cfg.ServeFrontend = false
		case "--disable-frontend":
			cfg.ServeFrontend = false
		}
	}
	// Populate the cross-language code_search_path from the
	// RAY_DASHBOARD_CODE_SEARCH_PATH env var, mirroring the Python dashboard
	// (head.py reads it into SubprocessModuleConfig so subprocesses like
	// ServeHead can load wrapper actor classes locally instead of from GCS).
	// Split on the OS path-list separator (':' on Linux) and drop empty entries.
	cfg.CodeSearchPath = splitCodeSearchPath(os.Getenv("RAY_DASHBOARD_CODE_SEARCH_PATH"))
	// Resolve the logging level default, aligned with the Python dashboard.py
	// argparse default (ray_constants.LOGGER_LEVEL = os.environ.get(
	// "RAY_LOGGER_LEVEL", "info")): an explicit --logging-level wins, otherwise
	// the RAY_LOGGER_LEVEL env var, otherwise "info".
	if cfg.LoggingLevel == "" {
		if v := os.Getenv("RAY_LOGGER_LEVEL"); v != "" {
			cfg.LoggingLevel = v
		} else {
			cfg.LoggingLevel = "info"
		}
	}
	return cfg, nil
}

// splitCodeSearchPath splits a path-list string (as set in
// RAY_DASHBOARD_CODE_SEARCH_PATH) on the OS path-list separator and drops empty
// entries, matching Python's `os.environ.get(...).split(os.pathsep)`.
func splitCodeSearchPath(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	var out []string
	for _, p := range filepath.SplitList(v) {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// splitModules parses the --modules-to-load value ("module1,module2" or empty)
// into a module name list, aligned with the Python
// `set(args.modules_to_load.strip(" ,").split(","))`.
func splitModules(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	var out []string
	for _, m := range strings.Split(v, ",") {
		if m = strings.TrimSpace(m); m != "" {
			out = append(out, m)
		}
	}
	return out
}
