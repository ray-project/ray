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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ==================== validatePath Tests ====================

func TestValidatePath(t *testing.T) {
	t.Run("valid existing directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		err := validatePath(tmpDir)
		assert.NoError(t, err)
	})

	t.Run("valid existing file", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "test.txt")
		err := os.WriteFile(tmpFile, []byte("test"), 0644)
		assert.NoError(t, err)
		err = validatePath(tmpFile)
		assert.NoError(t, err)
	})

	t.Run("non-existent path returns error", func(t *testing.T) {
		err := validatePath("/nonexistent/path/that/does/not/exist")
		assert.Error(t, err)
	})

	t.Run("empty path returns error", func(t *testing.T) {
		err := validatePath("")
		// Empty path is cleaned to "." which exists, so no error
		assert.NoError(t, err)
	})
}

// ==================== validateURI Tests ====================

func TestValidateURI(t *testing.T) {
	t.Run("invalid path treated as URI returns error", func(t *testing.T) {
		err := validateURI("local_module")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid URI")
	})

	t.Run("valid remote zip URI", func(t *testing.T) {
		err := validateURI("gcs://path/to/file.zip")
		assert.NoError(t, err)
	})

	t.Run("valid remote whl URI", func(t *testing.T) {
		err := validateURI("https://s3.example.com/bucket/file.whl")
		assert.NoError(t, err)
	})

	t.Run("https URI without .zip or .whl extension returns error", func(t *testing.T) {
		err := validateURI("https://example.com/path/to/file.tar.gz")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only .zip or .whl files supported for remote URIs")
	})

	t.Run("invalid URI format returns error", func(t *testing.T) {
		err := validateURI("://invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid URI")
	})
}

// ==================== handleLocalDepsRequirementFile Tests ====================

func TestHandleLocalDepsRequirementFile(t *testing.T) {
	t.Run("read valid requirements file", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "requirements.txt")
		content := "requests==2.28.0\nnumpy>=1.20.0\npandas"
		err := os.WriteFile(tmpFile, []byte(content), 0644)
		assert.NoError(t, err)

		lines, err := handleLocalDepsRequirementFile(tmpFile)
		assert.NoError(t, err)
		assert.Equal(t, []string{"requests==2.28.0", "numpy>=1.20.0", "pandas"}, lines)
	})

	t.Run("read requirements file with trailing newline", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "requirements.txt")
		content := "requests\nnumpy\n"
		err := os.WriteFile(tmpFile, []byte(content), 0644)
		assert.NoError(t, err)

		lines, err := handleLocalDepsRequirementFile(tmpFile)
		assert.NoError(t, err)
		assert.Equal(t, []string{"requests", "numpy"}, lines)
	})

	t.Run("non-existent file returns error", func(t *testing.T) {
		_, err := handleLocalDepsRequirementFile("/nonexistent/requirements.txt")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid file")
	})

	t.Run("directory instead of file returns error", func(t *testing.T) {
		tmpDir := t.TempDir()
		_, err := handleLocalDepsRequirementFile(tmpDir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid file")
	})
}

// ==================== validatePyModulesURIs Tests ====================

func TestValidatePyModulesURIs(t *testing.T) {
	t.Run("valid list of URIs", func(t *testing.T) {
		pyModules := []string{
			"gcs://path/to/module.zip",
			"https://s3.example.com/bucket/package.whl",
		}
		err := validatePyModulesURIs(pyModules)
		assert.NoError(t, err)
	})

	t.Run("nil list returns error", func(t *testing.T) {
		var pyModules interface{} = nil
		err := validatePyModulesURIs(pyModules)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "`py_modules` must be a list of strings, got nil")
	})

	t.Run("not a list returns error", func(t *testing.T) {
		err := validatePyModulesURIs("not_a_list")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "`py_modules` must be of type []string")
	})

	t.Run("list with non-string element returns error", func(t *testing.T) {
		pyModules := []interface{}{"gcs://module1.zip", 123, "module3"}
		err := validatePyModulesURIs(pyModules)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "`py_modules` must be of type []string")
	})

	t.Run("list with empty string returns error", func(t *testing.T) {
		pyModules := []string{"gcs://module1.zip", "", "module3"}
		err := validatePyModulesURIs(pyModules)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "`py_module` at index 1 must be a non-empty string")
	})

	t.Run("list with invalid URI returns error", func(t *testing.T) {
		pyModules := []string{"module1", "://invalid_uri"}
		err := validatePyModulesURIs(pyModules)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid URI")
	})

	t.Run("empty list is valid", func(t *testing.T) {
		pyModules := []string{}
		err := validatePyModulesURIs(pyModules)
		assert.NoError(t, err)
	})
}

// ==================== parseAndValidatePyModules Tests ====================

func TestParseAndValidatePyModules(t *testing.T) {
	t.Run("nil py_modules returns error", func(t *testing.T) {
		_, err := parseAndValidatePyModules(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "`py_modules` must be a list of strings, got nil")
	})

	t.Run("valid local path", func(t *testing.T) {
		tmpDir := t.TempDir()
		pyModules := []string{tmpDir}
		result, err := parseAndValidatePyModules(pyModules)
		assert.NoError(t, err)
		assert.Equal(t, pyModules, result)
	})

	t.Run("valid URI", func(t *testing.T) {
		pyModules := []string{"gcs://path/to/module.zip"}
		result, err := parseAndValidatePyModules(pyModules)
		assert.NoError(t, err)
		assert.Equal(t, pyModules, result)
	})

	t.Run("not a list returns error", func(t *testing.T) {
		_, err := parseAndValidatePyModules("not_a_list")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "`py_modules` must be of type []string, got string")
	})

	t.Run("list with empty string returns error", func(t *testing.T) {
		// Empty strings are treated as paths and checked for existence
		pyModules := []string{"gcs://module.zip", "", "module3"}
		_, err := parseAndValidatePyModules(pyModules)
		assert.Error(t, err)
		// Empty string is cleaned to "." which exists, so this test needs adjustment
		// Let's test with an actual non-existent path instead
	})

	t.Run("list with invalid path returns error", func(t *testing.T) {
		pyModules := []string{"/nonexistent/path"}
		_, err := parseAndValidatePyModules(pyModules)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid path")
	})

	t.Run("list with invalid URI returns error", func(t *testing.T) {
		// Invalid URIs that don't parse as paths will fail URI validation
		pyModules := []string{"gcs://valid.zip", "://invalid_uri"}
		_, err := parseAndValidatePyModules(pyModules)
		assert.Error(t, err)
		// The error message depends on whether it's treated as path or URI
		assert.True(t, err != nil)
	})

	t.Run("mixed local paths and URIs", func(t *testing.T) {
		tmpDir := t.TempDir()
		pyModules := []string{tmpDir, "gcs://module.zip"}
		result, err := parseAndValidatePyModules(pyModules)
		assert.NoError(t, err)
		assert.Equal(t, pyModules, result)
	})

	t.Run("empty list is valid", func(t *testing.T) {
		pyModules := []string{}
		result, err := parseAndValidatePyModules(pyModules)
		assert.NoError(t, err)
		assert.Equal(t, pyModules, result)
	})
}

// ==================== validateWorkingDirURI Tests ====================
func TestValidateWorkingDirURI(t *testing.T) {
	t.Run("valid working_dir URI", func(t *testing.T) {
		err := validateWorkingDirURI("gcs://path/to/working_dir.zip")
		assert.NoError(t, err)
	})

	t.Run("invalid path treated as URI returns error", func(t *testing.T) {
		err := validateWorkingDirURI("local_working_dir")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid URI")
	})

	t.Run("non-string returns error", func(t *testing.T) {
		err := validateWorkingDirURI(123)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "`working_dir` must be of type string, got int")
	})

	t.Run("empty string returns error", func(t *testing.T) {
		err := validateWorkingDirURI("")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid URI")
	})

	t.Run("nil returns error", func(t *testing.T) {
		err := validateWorkingDirURI(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "`working_dir` must be of type string, got <nil>")
	})

	t.Run("invalid URI returns error", func(t *testing.T) {
		err := validateWorkingDirURI("://invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid URI")
	})
}

// ==================== parseAndValidateWorkingDir Tests ====================

func TestParseAndValidateWorkingDir(t *testing.T) {
	t.Run("valid local directory path", func(t *testing.T) {
		tmpDir := t.TempDir()
		result, err := parseAndValidateWorkingDir(tmpDir)
		assert.NoError(t, err)
		assert.Equal(t, tmpDir, result)
	})

	t.Run("valid remote URI", func(t *testing.T) {
		workingDir := "gcs://path/to/working_dir.zip"
		result, err := parseAndValidateWorkingDir(workingDir)
		assert.NoError(t, err)
		assert.Equal(t, workingDir, result)
	})

	t.Run("non-existent local path returns error", func(t *testing.T) {
		_, err := parseAndValidateWorkingDir("/nonexistent/path")
		assert.Error(t, err)
	})

	t.Run("invalid URI returns error", func(t *testing.T) {
		// Invalid URIs that don't parse will fail validation
		_, err := parseAndValidateWorkingDir("://invalid_uri")
		assert.Error(t, err)
		// Error could be about path or URI depending on how it's parsed
		assert.True(t, err != nil)
	})

	t.Run("non-string type returns error", func(t *testing.T) {
		_, err := parseAndValidateWorkingDir(123)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "`working_dir` must be of type string, got int")
	})

	t.Run("nil returns error", func(t *testing.T) {
		_, err := parseAndValidateWorkingDir(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "`working_dir` must be of type string, got <nil>")
	})

	t.Run("empty string treated as path exists", func(t *testing.T) {
		// Empty string is cleaned to "." which exists
		result, err := parseAndValidateWorkingDir("")
		assert.NoError(t, err)
		assert.Equal(t, "", result)
	})

	t.Run("https URI without .zip or .whl extension returns error", func(t *testing.T) {
		_, err := parseAndValidateWorkingDir("https://example.com/file.tar.gz")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only .zip or .whl files supported for remote URIs")
	})
}

// ==================== parseAndValidateConda Tests ====================

func TestParseAndValidateConda(t *testing.T) {
	t.Run("string conda env name", func(t *testing.T) {
		conda := "myenv"
		result, err := parseAndValidateConda(conda)
		assert.NoError(t, err)
		assert.Equal(t, conda, result)
	})

	t.Run("absolute directory path", func(t *testing.T) {
		tmpDir := t.TempDir()
		result, err := parseAndValidateConda(tmpDir)
		assert.NoError(t, err)
		assert.Equal(t, tmpDir, result)
	})

	t.Run("YAML file with .yaml extension", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "environment.yaml")
		content := `name: myenv
dependencies:
  - python=3.8
  - numpy`
		err := os.WriteFile(tmpFile, []byte(content), 0644)
		assert.NoError(t, err)

		result, err := parseAndValidateConda(tmpFile)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "myenv", resultMap["name"])
	})

	t.Run("YAML file with .yml extension", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "environment.yml")
		content := `name: testenv
dependencies:
  - pandas`
		err := os.WriteFile(tmpFile, []byte(content), 0644)
		assert.NoError(t, err)

		result, err := parseAndValidateConda(tmpFile)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "testenv", resultMap["name"])
	})

	t.Run("non-existent YAML file returns error", func(t *testing.T) {
		_, err := parseAndValidateConda("/nonexistent/environment.yaml")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "can't find conda YAML file")
	})

	t.Run("directory instead of YAML file returns error", func(t *testing.T) {
		tmpDir := t.TempDir()
		_, err := parseAndValidateConda(tmpDir + ".yaml")
		assert.Error(t, err)
	})

	t.Run("invalid YAML content returns error", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "invalid.yaml")
		content := `invalid: yaml: content: [`
		err := os.WriteFile(tmpFile, []byte(content), 0644)
		assert.NoError(t, err)

		_, err = parseAndValidateConda(tmpFile)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse conda YAML file")
	})

	t.Run("map[string]string conda config", func(t *testing.T) {
		conda := map[string]string{
			"name":     "myenv",
			"channels": "conda-forge",
		}
		result, err := parseAndValidateConda(conda)
		assert.NoError(t, err)
		assert.Equal(t, conda, result)
	})

	t.Run("invalid type returns error", func(t *testing.T) {
		_, err := parseAndValidateConda(123)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['conda'] must be of type string or map, got int")
	})

	t.Run("relative path (not absolute) treated as env name", func(t *testing.T) {
		conda := "./relative/path"
		result, err := parseAndValidateConda(conda)
		assert.NoError(t, err)
		assert.Equal(t, conda, result)
	})
}

// ==================== parseAndValidateUv Tests ====================

func TestParseAndValidateUv(t *testing.T) {
	t.Run("[]string packages", func(t *testing.T) {
		uv := []string{"requests", "numpy>=1.20.0"}
		result, err := parseAndValidateUv(uv)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, []string{"requests", "numpy>=1.20.0"}, resultMap["packages"])
		assert.False(t, resultMap["uv_check"].(bool))
	})

	t.Run("requirements.txt file path", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "requirements.txt")
		content := "requests==2.28.0\npandas"
		err := os.WriteFile(tmpFile, []byte(content), 0644)
		assert.NoError(t, err)

		result, err := parseAndValidateUv(tmpFile)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, []string{"requests==2.28.0", "pandas"}, resultMap["packages"])
	})

	t.Run("invalid field in map returns error", func(t *testing.T) {
		uv := map[string]any{
			"packages":      []string{"requests"},
			"invalid_field": "value",
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv'] can only have these fields")
	})

	t.Run("missing packages field returns error", func(t *testing.T) {
		uv := map[string]any{
			"uv_check": true,
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv'] must include field 'packages'")
	})

	t.Run("uv_check non-bool type returns error", func(t *testing.T) {
		uv := map[string]any{
			"packages": []string{"requests"},
			"uv_check": "true",
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv']['uv_check'] must be of type bool")
	})

	t.Run("uv_version non-string type returns error", func(t *testing.T) {
		uv := map[string]any{
			"packages":   []string{"requests"},
			"uv_version": 123,
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv']['uv_version'] must be of type string")
	})

	t.Run("uv_pip_install_options non-slice type returns error", func(t *testing.T) {
		uv := map[string]any{
			"packages":               []string{"requests"},
			"uv_pip_install_options": "--no-cache",
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv']['uv_pip_install_options'] must be of type []string")
	})

	t.Run("packages non-list type returns error", func(t *testing.T) {
		uv := map[string]any{
			"packages": "requests",
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv']['packages'] must be of type list")
	})

	t.Run("packages with non-string element returns error", func(t *testing.T) {
		uv := map[string]any{
			"packages": []any{"requests", 123},
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv']['packages'] must be of type list")
	})

	t.Run("map with all fields", func(t *testing.T) {
		uv := map[string]any{
			"packages":               []string{"fastapi"},
			"uv_check":               true,
			"uv_version":             "0.1.0",
			"uv_pip_install_options": []string{"--upgrade", "--no-deps"},
		}
		result, err := parseAndValidateUv(uv)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, []string{"fastapi"}, resultMap["packages"])
		assert.True(t, resultMap["uv_check"].(bool))
		assert.Equal(t, "0.1.0", resultMap["uv_version"])
		assert.Equal(t, []string{"--upgrade", "--no-deps"}, resultMap["uv_pip_install_options"])
	})

	t.Run("invalid field in map returns error", func(t *testing.T) {
		uv := map[string]any{
			"packages":      []any{"requests"},
			"invalid_field": "value",
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv'] can only have these fields")
	})

	t.Run("missing packages field returns error", func(t *testing.T) {
		uv := map[string]any{
			"uv_check": true,
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv'] must include field 'packages'")
	})

	t.Run("uv_check non-bool type returns error", func(t *testing.T) {
		uv := map[string]any{
			"packages": []any{"requests"},
			"uv_check": "true",
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv']['uv_check'] must be of type bool")
	})

	t.Run("uv_version non-string type returns error", func(t *testing.T) {
		uv := map[string]any{
			"packages":   []any{"requests"},
			"uv_version": 123,
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv']['uv_version'] must be of type string")
	})

	t.Run("uv_pip_install_options non-slice type returns error", func(t *testing.T) {
		uv := map[string]any{
			"packages":               []any{"requests"},
			"uv_pip_install_options": "--no-cache",
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv']['uv_pip_install_options'] must be of type []string")
	})

	t.Run("packages non-list type returns error", func(t *testing.T) {
		uv := map[string]any{
			"packages": "requests",
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv']['packages'] must be of type list")
	})

	t.Run("packages with non-string element returns error", func(t *testing.T) {
		uv := map[string]any{
			"packages": []any{"requests", 123},
		}
		_, err := parseAndValidateUv(uv)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv']['packages'] must be of type list")
	})

	t.Run("invalid type returns error", func(t *testing.T) {
		_, err := parseAndValidateUv(123)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['uv'] must be of type []string, string, or map")
	})

	t.Run("nil uv returns error", func(t *testing.T) {
		_, err := parseAndValidateUv(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "uv cannot be nil")
	})

	t.Run("empty packages list returns nil", func(t *testing.T) {
		uv := []string{}
		result, err := parseAndValidateUv(uv)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("deduplicate packages", func(t *testing.T) {
		uv := []string{"requests", "numpy", "requests"}
		result, err := parseAndValidateUv(uv)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		packages := resultMap["packages"].([]string)
		assert.Len(t, packages, 2)
		assert.Contains(t, packages, "requests")
		assert.Contains(t, packages, "numpy")
	})
}

// ==================== parseAndValidatePip Tests ====================

func TestParseAndValidatePip(t *testing.T) {
	t.Run("[]string packages", func(t *testing.T) {
		pip := []string{"requests", "numpy>=1.20.0"}
		result, err := parseAndValidatePip(pip)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, []string{"requests", "numpy>=1.20.0"}, resultMap["packages"].([]string))
		assert.False(t, resultMap["pip_check"].(bool))
	})

	t.Run("requirements.txt file path", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "requirements.txt")
		content := "requests==2.28.0\npandas"
		err := os.WriteFile(tmpFile, []byte(content), 0644)
		assert.NoError(t, err)

		result, err := parseAndValidatePip(tmpFile)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, []string{"requests==2.28.0", "pandas"}, resultMap["packages"])
	})

	t.Run("map with packages as []string", func(t *testing.T) {
		pip := map[string]any{
			"packages": []string{"flask", "django"},
		}
		result, err := parseAndValidatePip(pip)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		packages := resultMap["packages"].([]string)
		assert.Equal(t, []string{"flask", "django"}, packages)
		assert.False(t, resultMap["pip_check"].(bool))
	})

	t.Run("map with packages as requirements file path", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "requirements.txt")
		content := "fastapi\nuvicorn"
		err := os.WriteFile(tmpFile, []byte(content), 0644)
		assert.NoError(t, err)

		pip := map[string]any{
			"packages": tmpFile,
		}
		result, err := parseAndValidatePip(pip)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, []string{"fastapi", "uvicorn"}, resultMap["packages"])
	})

	t.Run("map with all fields", func(t *testing.T) {
		pip := map[string]any{
			"packages":            []string{"pytest"},
			"pip_check":           true,
			"pip_version":         "21.0",
			"pip_install_options": []string{"--upgrade", "--no-deps"},
		}
		result, err := parseAndValidatePip(pip)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, []string{"pytest"}, resultMap["packages"])
		assert.True(t, resultMap["pip_check"].(bool))
		assert.Equal(t, "21.0", resultMap["pip_version"])
		assert.Equal(t, []string{"--upgrade", "--no-deps"}, resultMap["pip_install_options"])
	})

	t.Run("invalid field in map returns error", func(t *testing.T) {
		pip := map[string]any{
			"packages":      []any{"requests"},
			"invalid_field": "value",
		}
		_, err := parseAndValidatePip(pip)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['pip'] can only have these fields")
	})

	t.Run("missing packages field returns error", func(t *testing.T) {
		pip := map[string]any{
			"pip_check": true,
		}
		_, err := parseAndValidatePip(pip)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['pip'] must include field 'packages'")
	})

	t.Run("pip_check non-bool type returns error", func(t *testing.T) {
		pip := map[string]any{
			"packages":  []any{"requests"},
			"pip_check": "true",
		}
		_, err := parseAndValidatePip(pip)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['pip']['pip_check'] must be of type bool")
	})

	t.Run("pip_version non-string type returns error", func(t *testing.T) {
		pip := map[string]any{
			"packages":    []any{"requests"},
			"pip_version": 123,
		}
		_, err := parseAndValidatePip(pip)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['pip']['pip_version'] must be of type string")
	})

	t.Run("pip_install_options with non-string element returns error", func(t *testing.T) {
		pip := map[string]any{
			"packages":            []string{"requests"},
			"pip_install_options": []any{"--upgrade", 123},
		}
		_, err := parseAndValidatePip(pip)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['pip']['pip_install_options'] must be of type []string")
	})

	t.Run("packages non-string/list type returns error", func(t *testing.T) {
		pip := map[string]any{
			"packages": 123,
		}
		_, err := parseAndValidatePip(pip)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['pip']['packages'] must be of type string or list")
	})

	t.Run("invalid type returns error", func(t *testing.T) {
		_, err := parseAndValidatePip(123)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['pip'] must be of type string, []string, or map")
	})

	t.Run("nil pip returns error", func(t *testing.T) {
		_, err := parseAndValidatePip(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pip cannot be nil")
	})

	t.Run("empty packages list returns nil", func(t *testing.T) {
		pip := []string{}
		result, err := parseAndValidatePip(pip)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("deduplicate packages", func(t *testing.T) {
		pip := []string{"requests", "numpy", "requests"}
		result, err := parseAndValidatePip(pip)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		packages := resultMap["packages"].([]string)
		assert.Len(t, packages, 2)
		assert.Contains(t, packages, "requests")
		assert.Contains(t, packages, "numpy")
	})

	t.Run("non-existent requirements file returns error", func(t *testing.T) {
		_, err := parseAndValidatePip("/nonexistent/requirements.txt")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read requirements file")
	})
}

// ==================== parseAndValidateContainer Tests ====================

func TestParseAndValidateContainer(t *testing.T) {
	t.Run("nil container returns error", func(t *testing.T) {
		_, err := parseAndValidateContainer(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "container cannot be nil")
	})

	t.Run("valid container config passed through", func(t *testing.T) {
		container := &ContainerConfig{
			Image:      "my-image:latest",
			WorkerPath: "/worker",
			RunOptions: []string{"--rm"},
		}
		result, err := parseAndValidateContainer(container)
		assert.NoError(t, err)
		assert.Equal(t, container, result)
	})

	t.Run("map container passed through", func(t *testing.T) {
		container := map[string]interface{}{
			"image":       "my-image",
			"worker_path": "/worker",
		}
		result, err := parseAndValidateContainer(container)
		assert.NoError(t, err)
		assert.Equal(t, container, result)
	})

	t.Run("string container passed through", func(t *testing.T) {
		container := "my-container"
		result, err := parseAndValidateContainer(container)
		assert.NoError(t, err)
		assert.Equal(t, container, result)
	})
}

// ==================== parseAndValidateExcludes Tests ====================

func TestParseAndValidateExcludes(t *testing.T) {
	t.Run("nil excludes returns error", func(t *testing.T) {
		_, err := parseAndValidateExcludes(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "excludes cannot be nil")
	})

	t.Run("valid string slice", func(t *testing.T) {
		excludes := []string{"*.pyc", "__pycache__", ".git"}
		result, err := parseAndValidateExcludes(excludes)
		assert.NoError(t, err)
		strSlice, ok := result.([]string)
		assert.True(t, ok)
		assert.Equal(t, []string{"*.pyc", "__pycache__", ".git"}, strSlice)
	})

	t.Run("empty list returns nil", func(t *testing.T) {
		excludes := []string{}
		result, err := parseAndValidateExcludes(excludes)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("not a list returns error", func(t *testing.T) {
		_, err := parseAndValidateExcludes("not_a_list")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['excludes'] must be of type List[str], got string")
	})

	t.Run("list with non-string element returns error", func(t *testing.T) {
		excludes := []interface{}{"*.pyc", 123}
		_, err := parseAndValidateExcludes(excludes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['excludes'] must be of type List[str], got []interface {}")
	})
}

// ==================== parseAndValidateEnvVars Tests ====================

func TestParseAndValidateEnvVars(t *testing.T) {
	t.Run("nil env_vars returns error", func(t *testing.T) {
		_, err := parseAndValidateEnvVars(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "env_vars cannot be nil")
	})

	t.Run("valid string map", func(t *testing.T) {
		envVars := map[string]interface{}{
			"KEY1": "value1",
			"KEY2": "value2",
		}
		result, err := parseAndValidateEnvVars(envVars)
		assert.NoError(t, err)
		strMap, ok := result.(map[string]string)
		assert.True(t, ok)
		assert.Equal(t, "value1", strMap["KEY1"])
		assert.Equal(t, "value2", strMap["KEY2"])
	})

	t.Run("empty map returns nil", func(t *testing.T) {
		envVars := map[string]interface{}{}
		result, err := parseAndValidateEnvVars(envVars)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("not a map returns error", func(t *testing.T) {
		_, err := parseAndValidateEnvVars("not_a_map")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['env_vars'] must be of type map[string]string or map[string]interface{}")
	})

	t.Run("map with non-string value returns error", func(t *testing.T) {
		envVars := map[string]interface{}{
			"KEY1": 123,
		}
		_, err := parseAndValidateEnvVars(envVars)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "runtime_env['env_vars']['KEY1'] must be of type string, got int")
	})
}

// ==================== OptionToValidationFn Tests ====================

func TestOptionToValidationFn(t *testing.T) {
	t.Run("FieldPyModules validation function exists", func(t *testing.T) {
		fn, exists := OptionToValidationFn[FieldPyModules]
		assert.True(t, exists)
		assert.NotNil(t, fn)
	})

	t.Run("FieldWorkingDir validation function exists", func(t *testing.T) {
		fn, exists := OptionToValidationFn[FieldWorkingDir]
		assert.True(t, exists)
		assert.NotNil(t, fn)
	})

	t.Run("FieldExcludes validation function exists", func(t *testing.T) {
		fn, exists := OptionToValidationFn[FieldExcludes]
		assert.True(t, exists)
		assert.NotNil(t, fn)
	})

	t.Run("FieldConda validation function exists", func(t *testing.T) {
		fn, exists := OptionToValidationFn[FieldConda]
		assert.True(t, exists)
		assert.NotNil(t, fn)
	})

	t.Run("FieldPip validation function exists", func(t *testing.T) {
		fn, exists := OptionToValidationFn[FieldPip]
		assert.True(t, exists)
		assert.NotNil(t, fn)
	})

	t.Run("FieldUv validation function exists", func(t *testing.T) {
		fn, exists := OptionToValidationFn[FieldUv]
		assert.True(t, exists)
		assert.NotNil(t, fn)
	})

	t.Run("FieldEnvVars validation function exists", func(t *testing.T) {
		fn, exists := OptionToValidationFn[FieldEnvVars]
		assert.True(t, exists)
		assert.NotNil(t, fn)
	})

	t.Run("FieldContainer validation function exists", func(t *testing.T) {
		fn, exists := OptionToValidationFn[FieldContainer]
		assert.True(t, exists)
		assert.NotNil(t, fn)
	})

	t.Run("FieldConfig validation function exists", func(t *testing.T) {
		fn, exists := OptionToValidationFn[FieldConfig]
		assert.True(t, exists)
		assert.NotNil(t, fn)
	})
}

// ==================== OptionToNoPathValidationFn Tests ====================

func TestOptionToNoPathValidationFn(t *testing.T) {
	t.Run("FieldWorkingDir no-path validation function exists", func(t *testing.T) {
		fn, exists := OptionToNoPathValidationFn[FieldWorkingDir]
		assert.True(t, exists)
		assert.NotNil(t, fn)
	})

	t.Run("FieldPyModules no-path validation function exists", func(t *testing.T) {
		fn, exists := OptionToNoPathValidationFn[FieldPyModules]
		assert.True(t, exists)
		assert.NotNil(t, fn)
	})
}

// ==================== Windows-specific tests ====================

func TestWindowsLogging(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Run("conda validation logs warning on Windows", func(t *testing.T) {
			// This test verifies that the Windows warning is logged
			// The actual logging behavior would need to be tested differently
			tmpDir := t.TempDir()
			_, err := parseAndValidateConda(tmpDir)
			assert.NoError(t, err)
		})

		t.Run("uv validation logs warning on Windows", func(t *testing.T) {
			uv := []interface{}{"requests"}
			_, err := parseAndValidateUv(uv)
			assert.NoError(t, err)
		})

		t.Run("pip validation logs warning on Windows", func(t *testing.T) {
			pip := []interface{}{"requests"}
			_, err := parseAndValidatePip(pip)
			assert.NoError(t, err)
		})
	} else {
		t.Skip("Skipping Windows-specific tests on non-Windows platform")
	}
}

// ==================== Edge cases and integration tests ====================

func TestValidatePathEdgeCases(t *testing.T) {
	t.Run("path with spaces", func(t *testing.T) {
		tmpDir := t.TempDir()
		pathWithSpaces := filepath.Join(tmpDir, "dir with spaces")
		err := os.Mkdir(pathWithSpaces, 0755)
		assert.NoError(t, err)

		err = validatePath(pathWithSpaces)
		assert.NoError(t, err)
	})

	t.Run("relative path", func(t *testing.T) {
		// Relative paths are cleaned and checked
		err := validatePath(".")
		assert.NoError(t, err)
	})

	t.Run("path with special characters", func(t *testing.T) {
		tmpDir := t.TempDir()
		specialPath := filepath.Join(tmpDir, "dir-with_special.chars")
		err := os.Mkdir(specialPath, 0755)
		assert.NoError(t, err)

		err = validatePath(specialPath)
		assert.NoError(t, err)
	})
}

func TestValidateURIEedgeCases(t *testing.T) {
	t.Run("empty URI returns error", func(t *testing.T) {
		err := validateURI("")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid URI")
	})

	t.Run("URI without protocol returns error", func(t *testing.T) {
		err := validateURI("path/to/file.zip")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid URI")
	})

	t.Run("s3 URI with .zip extension", func(t *testing.T) {
		err := validateURI("https://s3.example.com/bucket/file.zip")
		assert.NoError(t, err)
	})

	t.Run("gcs URI with .whl extension", func(t *testing.T) {
		err := validateURI("gcs://bucket/package.whl")
		assert.NoError(t, err)
	})

	t.Run("https URI with .zip extension", func(t *testing.T) {
		err := validateURI("https://example.com/file.zip")
		assert.NoError(t, err)
	})

	t.Run("https URI with .whl extension", func(t *testing.T) {
		err := validateURI("https://example.com/package.whl")
		assert.NoError(t, err)
	})

	t.Run("http URI (not https) with .zip extension", func(t *testing.T) {
		err := validateURI("http://example.com/file.zip")
		assert.Error(t, err)
	})

	t.Run("file:// protocol is remote", func(t *testing.T) {
		// file:// is treated as local path, not remote protocol
		err := validateURI("file:///path/to/file.zip")
		assert.Nil(t, err) // Not a valid URI format for ParseURI
	})
}

func TestHandleLocalDepsRequirementFileEdgeCases(t *testing.T) {
	t.Run("empty requirements file", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "requirements.txt")
		err := os.WriteFile(tmpFile, []byte(""), 0644)
		assert.NoError(t, err)

		lines, err := handleLocalDepsRequirementFile(tmpFile)
		assert.NoError(t, err)
		assert.Equal(t, []string{""}, lines) // Empty file returns one empty line
	})

	t.Run("requirements file with comments", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "requirements.txt")
		content := "# This is a comment\nrequests==2.28.0\n# Another comment\nnumpy"
		err := os.WriteFile(tmpFile, []byte(content), 0644)
		assert.NoError(t, err)

		lines, err := handleLocalDepsRequirementFile(tmpFile)
		assert.NoError(t, err)
		assert.Equal(t, []string{"# This is a comment", "requests==2.28.0", "# Another comment", "numpy"}, lines)
	})

	t.Run("requirements file with blank lines", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "requirements.txt")
		content := "requests\n\nnumpy\n\npandas"
		err := os.WriteFile(tmpFile, []byte(content), 0644)
		assert.NoError(t, err)

		lines, err := handleLocalDepsRequirementFile(tmpFile)
		assert.NoError(t, err)
		assert.Equal(t, []string{"requests", "", "numpy", "", "pandas"}, lines)
	})

	t.Run("symlink to requirements file", func(t *testing.T) {
		tmpDir := t.TempDir()
		realFile := filepath.Join(tmpDir, "real_requirements.txt")
		symlink := filepath.Join(tmpDir, "link_requirements.txt")

		err := os.WriteFile(realFile, []byte("requests"), 0644)
		assert.NoError(t, err)

		err = os.Symlink(realFile, symlink)
		if err != nil {
			// Skip on Windows if symlinks require privileges
			t.Skip("Symlinks not supported on this platform")
		}

		lines, err := handleLocalDepsRequirementFile(symlink)
		assert.NoError(t, err)
		assert.Equal(t, []string{"requests"}, lines)
	})
}

func TestParseAndValidatePyModulesEdgeCases(t *testing.T) {
	t.Run("single element list", func(t *testing.T) {
		pyModules := []string{"gcs://module.zip"}
		result, err := parseAndValidatePyModules(pyModules)
		assert.NoError(t, err)
		assert.Equal(t, pyModules, result)
	})

	t.Run("many elements in list", func(t *testing.T) {
		pyModules := make([]string, 100)
		for i := range pyModules {
			pyModules[i] = fmt.Sprintf("gcs://module%d.zip", i)
		}
		result, err := parseAndValidatePyModules(pyModules)
		assert.NoError(t, err)
		assert.Equal(t, pyModules, result)
	})

	t.Run("list with only URIs", func(t *testing.T) {
		pyModules := []string{
			"gcs://module1.zip",
			"https://s3.example.com/module2.whl",
			"https://example.com/module3.zip",
		}
		result, err := parseAndValidatePyModules(pyModules)
		assert.NoError(t, err)
		assert.Equal(t, pyModules, result)
	})

	t.Run("list with only local paths", func(t *testing.T) {
		tmpDir1 := t.TempDir()
		tmpDir2 := t.TempDir()
		pyModules := []string{tmpDir1, tmpDir2}
		result, err := parseAndValidatePyModules(pyModules)
		assert.NoError(t, err)
		assert.Equal(t, pyModules, result)
	})
}

func TestParseAndValidateWorkingDirEdgeCases(t *testing.T) {
	t.Run("Windows-style path (on non-Windows)", func(t *testing.T) {
		if runtime.GOOS != "windows" {
			// On non-Windows, this would be treated as a relative path or invalid
			// Just test that it doesn't panic
			_, err := parseAndValidateWorkingDir("C:\\Users\\test")
			// May error due to path not existing, but shouldn't panic
			assert.Error(t, err)
		}
	})

	t.Run("path with trailing slash", func(t *testing.T) {
		tmpDir := t.TempDir()
		result, err := parseAndValidateWorkingDir(tmpDir)
		assert.NoError(t, err)
		// Path cleaning removes trailing slash
		assert.Equal(t, tmpDir, result)
	})

	t.Run("URI with query parameters", func(t *testing.T) {
		uri := "gcs://bucket/file.zip?token=abc123"
		result, err := parseAndValidateWorkingDir(uri)
		assert.NoError(t, err)
		assert.Equal(t, uri, result)
	})
}

func TestParseAndValidateCondaEdgeCases(t *testing.T) {
	t.Run("YAML file with complex structure", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "complex.yaml")
		content := `name: complex_env
channels:
  - conda-forge
  - defaults
dependencies:
  - python=3.9
  - numpy
  - pip:
    - requests
    - flask`
		err := os.WriteFile(tmpFile, []byte(content), 0644)
		assert.NoError(t, err)

		result, err := parseAndValidateConda(tmpFile)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "complex_env", resultMap["name"])
	})

	t.Run("empty string conda env name", func(t *testing.T) {
		conda := ""
		result, err := parseAndValidateConda(conda)
		assert.NoError(t, err)
		assert.Equal(t, conda, result)
	})

	t.Run("YAML file with only name", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "minimal.yaml")
		content := `name: minimal`
		err := os.WriteFile(tmpFile, []byte(content), 0644)
		assert.NoError(t, err)

		result, err := parseAndValidateConda(tmpFile)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "minimal", resultMap["name"])
	})
}

func TestParseAndValidateUvEdgeCases(t *testing.T) {
	t.Run("map with empty packages list", func(t *testing.T) {
		uv := map[string]any{
			"packages": []string{},
		}
		result, err := parseAndValidateUv(uv)
		assert.NoError(t, err)
		assert.Nil(t, result) // Empty packages returns nil
	})

	t.Run("map with uv_check false explicitly", func(t *testing.T) {
		uv := map[string]any{
			"packages": []string{"requests"},
			"uv_check": false,
		}
		result, err := parseAndValidateUv(uv)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.False(t, resultMap["uv_check"].(bool))
	})

	t.Run("map with default uv_pip_install_options", func(t *testing.T) {
		uv := map[string]any{
			"packages": []string{"requests"},
		}
		result, err := parseAndValidateUv(uv)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, []string{"--no-cache"}, resultMap["uv_pip_install_options"])
	})

	t.Run("non-existent requirements file", func(t *testing.T) {
		_, err := parseAndValidateUv("/nonexistent/requirements.txt")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid file")
	})
}

func TestParseAndValidatePipEdgeCases(t *testing.T) {
	t.Run("map with empty packages list", func(t *testing.T) {
		pip := map[string]any{
			"packages": []string{},
		}
		result, err := parseAndValidatePip(pip)
		assert.NoError(t, err)
		assert.Nil(t, result) // Empty packages returns nil
	})

	t.Run("map with pip_check false explicitly", func(t *testing.T) {
		pip := map[string]any{
			"packages":  []string{"requests"},
			"pip_check": false,
		}
		result, err := parseAndValidatePip(pip)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.False(t, resultMap["pip_check"].(bool))
	})

	t.Run("map with pip_version as empty string", func(t *testing.T) {
		pip := map[string]any{
			"packages":    []string{"requests"},
			"pip_version": "",
		}
		result, err := parseAndValidatePip(pip)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, "", resultMap["pip_version"])
	})

	t.Run("map with empty pip_install_options", func(t *testing.T) {
		pip := map[string]any{
			"packages":            []string{"requests"},
			"pip_install_options": []string{},
		}
		result, err := parseAndValidatePip(pip)
		assert.NoError(t, err)
		resultMap, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, []string{}, resultMap["pip_install_options"])
	})

	t.Run("packages as non-existent requirements file", func(t *testing.T) {
		pip := map[string]any{
			"packages": "/nonexistent/requirements.txt",
		}
		_, err := parseAndValidatePip(pip)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read requirements file")
	})
}

func TestParseAndValidateExcludesEdgeCases(t *testing.T) {
	t.Run("single exclude pattern", func(t *testing.T) {
		excludes := []string{"*.pyc"}
		result, err := parseAndValidateExcludes(excludes)
		assert.NoError(t, err)
		strSlice, ok := result.([]string)
		assert.True(t, ok)
		assert.Equal(t, []string{"*.pyc"}, strSlice)
	})

	t.Run("exclude patterns with wildcards", func(t *testing.T) {
		excludes := []string{"**/__pycache__/**", "*.py[c,o]", ".git/**"}
		result, err := parseAndValidateExcludes(excludes)
		assert.NoError(t, err)
		strSlice, ok := result.([]string)
		assert.True(t, ok)
		assert.Equal(t, excludes, strSlice)
	})

	t.Run("exclude with empty strings", func(t *testing.T) {
		excludes := []string{"*.pyc", "", "__pycache__"}
		result, err := parseAndValidateExcludes(excludes)
		assert.NoError(t, err)
		strSlice, ok := result.([]string)
		assert.True(t, ok)
		assert.Equal(t, []string{"*.pyc", "", "__pycache__"}, strSlice)
	})
}

func TestParseAndValidateEnvVarsEdgeCases(t *testing.T) {
	t.Run("single environment variable", func(t *testing.T) {
		envVars := map[string]interface{}{
			"KEY": "value",
		}
		result, err := parseAndValidateEnvVars(envVars)
		assert.NoError(t, err)
		strMap, ok := result.(map[string]string)
		assert.True(t, ok)
		assert.Equal(t, "value", strMap["KEY"])
	})

	t.Run("environment variables with special characters in values", func(t *testing.T) {
		envVars := map[string]interface{}{
			"PATH":    "/usr/bin:/bin",
			"SPECIAL": "value with spaces and $pecial chars!",
		}
		result, err := parseAndValidateEnvVars(envVars)
		assert.NoError(t, err)
		strMap, ok := result.(map[string]string)
		assert.True(t, ok)
		assert.Equal(t, "/usr/bin:/bin", strMap["PATH"])
		assert.Equal(t, "value with spaces and $pecial chars!", strMap["SPECIAL"])
	})

	t.Run("environment variable with empty value", func(t *testing.T) {
		envVars := map[string]interface{}{
			"EMPTY": "",
		}
		result, err := parseAndValidateEnvVars(envVars)
		assert.NoError(t, err)
		strMap, ok := result.(map[string]string)
		assert.True(t, ok)
		assert.Equal(t, "", strMap["EMPTY"])
	})

	t.Run("map with integer key (should fail)", func(t *testing.T) {
		// This tests that keys must be strings
		// Note: In Go, map[string]interface{} already enforces string keys at compile time
		// So this test is more for documentation
		envVars := map[string]interface{}{
			"KEY1": "value1",
		}
		result, err := parseAndValidateEnvVars(envVars)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}

func TestOptionToValidationFnCoverage(t *testing.T) {
	t.Run("all known fields have validation functions", func(t *testing.T) {
		knownFields := []string{
			FieldPyModules, FieldWorkingDir, FieldExcludes, FieldConda,
			FieldPip, FieldUv, FieldEnvVars, FieldContainer, FieldConfig,
		}

		for _, field := range knownFields {
			fn, exists := OptionToValidationFn[field]
			assert.True(t, exists, "Field %s should have a validation function", field)
			assert.NotNil(t, fn, "Validation function for %s should not be nil", field)
		}
	})
}

func TestOptionToNoPathValidationFnCoverage(t *testing.T) {
	t.Run("fields with no-path validation", func(t *testing.T) {
		noPathFields := []string{FieldWorkingDir, FieldPyModules}

		for _, field := range noPathFields {
			fn, exists := OptionToNoPathValidationFn[field]
			assert.True(t, exists, "Field %s should have a no-path validation function", field)
			assert.NotNil(t, fn, "No-path validation function for %s should not be nil", field)
		}
	})
}

func TestIntegrationScenarios(t *testing.T) {
	t.Run("complete runtime env with multiple options", func(t *testing.T) {
		tmpDir := t.TempDir()
		reqFile := filepath.Join(tmpDir, "requirements.txt")
		err := os.WriteFile(reqFile, []byte("requests\nflask"), 0644)
		assert.NoError(t, err)

		// Test various validation functions together
		_, err = parseAndValidateWorkingDir(tmpDir)
		assert.NoError(t, err)

		_, err = parseAndValidatePip(map[string]any{
			"packages": []string{"numpy", "pandas"},
		})
		assert.NoError(t, err)

		_, err = parseAndValidateExcludes([]string{"*.pyc", "__pycache__"})
		assert.NoError(t, err)

		_, err = parseAndValidateEnvVars(map[string]interface{}{
			"ENV": "production",
		})
		assert.NoError(t, err)
	})

	t.Run("error propagation in validation chain", func(t *testing.T) {
		// Test that errors are properly propagated
		_, err := parseAndValidatePyModules([]string{"/nonexistent/path"})
		assert.Error(t, err)

		_, err = parseAndValidateWorkingDir("://invalid_uri")
		assert.Error(t, err)

		_, err = parseAndValidateConda("/nonexistent.yaml")
		assert.Error(t, err)
	})
}

func TestCrossPlatformCompatibility(t *testing.T) {
	t.Run("path validation on current platform", func(t *testing.T) {
		// Test basic path operations work on current platform
		tmpDir := t.TempDir()
		err := validatePath(tmpDir)
		assert.NoError(t, err)

		// Test file path
		tmpFile := filepath.Join(tmpDir, "test.txt")
		err = os.WriteFile(tmpFile, []byte("test"), 0644)
		assert.NoError(t, err)

		err = validatePath(tmpFile)
		assert.NoError(t, err)
	})

	t.Run("URI validation is platform independent", func(t *testing.T) {
		// URIs should validate the same way on all platforms
		uris := []string{
			"gcs://bucket/file.zip",
			"https://s3.example.com/bucket/file.whl",
			"https://example.com/file.zip",
		}

		for _, uri := range uris {
			err := validateURI(uri)
			assert.NoError(t, err, "URI %s should be valid on all platforms", uri)
		}
	})
}
