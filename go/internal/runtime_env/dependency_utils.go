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

// Package runtime_env provides runtime environment configuration for Ray workers.
package runtime_env

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ray-project/ray/go/pkg/log"
)

func genRequirementsTxt(requirementsFile string, pipPackages []string) error {
	requirementsContent := strings.Join(pipPackages, "\n")
	if err := os.WriteFile(requirementsFile, []byte(requirementsContent), 0644); err != nil {
		return fmt.Errorf("failed to write requirements file: %w", err)
	}
	return nil
}

func checkRay(ctx context.Context, python string, cwd string) error {
	cmdIndexGen := newCmdIndexGen()

	getRayVersionAndPath := func() (string, string, error) {
		tmpDir, err := os.MkdirTemp("", "check_ray_version_tempfile")
		if err != nil {
			return "", "", err
		}
		defer os.RemoveAll(tmpDir)

		rayVersionPath := filepath.Join(tmpDir, "ray_version.txt")
		checkRayCmd := []string{
			python,
			"-c",
			fmt.Sprintf(`
import ray
with open(%q, "wt") as f:
    f.write(ray.__version__)
    f.write(" ")
    f.write(ray.__path__[0])
`, rayVersionPath),
		}

		env := make(map[string]string)
		if runtime.GOOS == "windows" {
			for _, e := range os.Environ() {
				parts := strings.SplitN(e, "=", 2)
				if len(parts) == 2 {
					env[parts[0]] = parts[1]
				}
			}
		}

		_, err = checkOutputCmd(ctx, checkRayCmd, cwd, env, cmdIndexGen)
		if err != nil {
			return "", "", err
		}

		content, err := os.ReadFile(rayVersionPath)
		if err != nil {
			return "", "", err
		}

		parts := strings.Fields(strings.TrimSpace(string(content)))
		if len(parts) < 2 {
			return "", "", fmt.Errorf("unexpected ray version output: %s", string(content))
		}

		return parts[0], parts[1], nil
	}

	version, path, err := getRayVersionAndPath()
	if err != nil {
		return err
	}

	actualVersion, actualPath := version, path

	if actualVersion != version {
		return fmt.Errorf("Changing the ray version is not allowed:\n  current version: %s, expect version: %s, current path: %s, expect path: %s, Please ensure the dependencies in the runtime_env pip field do not install a different version of Ray.",
			actualVersion, version, actualPath, path)
	}

	if actualPath != path {
		log.Log.V(1).Info("Detected new Ray package with the same version at actual_path (vs system path)",
			"actual_path", actualPath, "path", path)
	}

	return nil
}

func getRequirementsFile(targetDir string, pipList []string) (string, error) {
	internalPipFilename := "ray_runtime_env_internal_pip_requirements.txt"
	maxTries := 100

	filenameInPipList := func(filename string) bool {
		for _, entry := range pipList {
			if strings.Contains(entry, filename) {
				return true
			}
		}
		return false
	}

	filename := internalPipFilename
	if pipList != nil {
		i := 1
		for filenameInPipList(filename) && i < maxTries {
			filename = fmt.Sprintf("%s.%d", internalPipFilename, i)
			i++
		}
		if i == maxTries {
			return "", fmt.Errorf("could not find a valid filename for the internal pip requirements file. Please specify a different pip list in your runtime env")
		}
	}

	return filepath.Join(targetDir, filename), nil
}
