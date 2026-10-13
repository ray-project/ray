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

package runtime_env

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTempSchemaFile creates a temporary schema file and returns its path.
func createTempSchemaFile(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test_schema.json")
	err := os.WriteFile(tmpFile, []byte(content), 0o644)
	require.NoError(t, err)
	return tmpFile
}

// cleanupEnv clears the environment variable setting.
func cleanupEnv(t *testing.T) {
	t.Helper()
	os.Unsetenv(RayRuntimeEnvPluginSchemasEnvVar)
	Clear() // Clear the loaded schemas.
}

// TestLoadSchemasFromEnvVar_File tests loading a single file from the
// environment variable.
func TestLoadSchemasFromEnvVar_File(t *testing.T) {
	defer cleanupEnv(t)

	// Create a temporary schema file.
	schemaContent := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "env_test",
		"type": "string"
	}`
	tmpFile := createTempSchemaFile(t, schemaContent)

	// Set the environment variable.
	os.Setenv(RayRuntimeEnvPluginSchemasEnvVar, tmpFile)

	// Load the schema.
	loadSchemasFromEnvVar()

	// Verify the schema was loaded.
	schemaMu.RLock()
	_, exists := schemas["env_test"]
	schemaMu.RUnlock()
	assert.True(t, exists)
}

// TestLoadSchemas_InvalidJSON tests handling of an invalid JSON file.
func TestLoadSchemas_InvalidJSON(t *testing.T) {
	defer cleanupEnv(t)

	// Create an invalid JSON file.
	invalidJSON := `{"title": "invalid", invalid json here}`
	tmpFile := createTempSchemaFile(t, invalidJSON)

	// Calling loadSingleSchema directly should return an error.
	err := loadSingleSchema(tmpFile)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse JSON")
}

// TestLoadSchemas_MissingTitle tests a schema without a title field.
func TestLoadSchemas_MissingTitle(t *testing.T) {
	defer cleanupEnv(t)

	// Create a schema without a title.
	noTitleSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "string"
	}`
	tmpFile := createTempSchemaFile(t, noTitleSchema)

	err := loadSingleSchema(tmpFile)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no valid title")
}

// TestLoadSchemas_TitleConflict tests title conflict handling.
func TestLoadSchemas_TitleConflict(t *testing.T) {
	defer cleanupEnv(t)

	// Create two schemas with the same title.
	schema1 := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "conflict_test",
		"type": "string"
	}`
	schema2 := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "conflict_test",
		"type": "integer"
	}`

	tmpFile1 := createTempSchemaFile(t, schema1)
	tmpFile2 := createTempSchemaFile(t, schema2)

	// Load the first schema.
	err := loadSingleSchema(tmpFile1)
	require.NoError(t, err)

	// Loading the second schema should fail (title conflict).
	err = loadSingleSchema(tmpFile2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "conflicts with previously loaded schema")
}

// TestValidate_Success tests a successful validation case.
func TestValidate_Success(t *testing.T) {
	defer cleanupEnv(t)

	// Create a simple schema for the test.
	schemaContent := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "simple_test",
		"type": "object",
		"properties": {
			"name": {"type": "string"}
		},
		"required": ["name"]
	}`
	tmpFile := createTempSchemaFile(t, schemaContent)
	os.Setenv(RayRuntimeEnvPluginSchemasEnvVar, tmpFile)
	loadSchemasFromEnvVar()

	// A valid instance.
	instance := map[string]interface{}{"name": "test"}
	err := Validate("simple_test", instance)
	assert.NoError(t, err)
}

// TestValidate_Failure tests a failed validation case.
func TestValidate_Failure(t *testing.T) {
	defer cleanupEnv(t)

	// Create a simple schema for the test.
	schemaContent := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "failure_test",
		"type": "object",
		"properties": {
			"name": {"type": "string"}
		},
		"required": ["name"]
	}`
	tmpFile := createTempSchemaFile(t, schemaContent)
	os.Setenv(RayRuntimeEnvPluginSchemasEnvVar, tmpFile)
	loadSchemasFromEnvVar()

	// An invalid instance (missing the required name field).
	instance := map[string]interface{}{"value": 42}
	err := Validate("failure_test", instance)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")
}

// TestValidate_NoMatchingSchema tests the case with no matching schema.
func TestValidate_NoMatchingSchema(t *testing.T) {
	defer cleanupEnv(t)

	// Validating a non-existent schema should skip validation (not an error).
	err := Validate("non_existent_schema", map[string]interface{}{"anything": "goes"})
	assert.NoError(t, err)
}

// TestClear verifies that Clear() resets the state.
func TestClear(t *testing.T) {
	defer cleanupEnv(t)

	// Load some schemas first.
	schemaContent := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "clear_test",
		"type": "string"
	}`
	tmpFile := createTempSchemaFile(t, schemaContent)
	os.Setenv(RayRuntimeEnvPluginSchemasEnvVar, tmpFile)

	loadSchemasFromEnvVar()

	// Verify the schema was loaded.
	schemaMu.RLock()
	_, exists := schemas["clear_test"]
	schemaMu.RUnlock()
	assert.True(t, exists)

	// Call Clear.
	Clear()

	// Verify the schema was cleared.
	schemaMu.RLock()
	_, exists = schemas["clear_test"]
	loadedState := loaded
	schemaMu.RUnlock()
	assert.False(t, exists)
	assert.False(t, loadedState)
}

// TestConcurrentValidate has multiple goroutines call Validate concurrently.
func TestConcurrentValidate(t *testing.T) {
	defer cleanupEnv(t)

	// Create a simple schema for the concurrency test.
	schemaContent := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "concurrent_test",
		"type": "object",
		"properties": {
			"id": {"type": "integer"}
		}
	}`
	tmpFile := createTempSchemaFile(t, schemaContent)
	os.Setenv(RayRuntimeEnvPluginSchemasEnvVar, tmpFile)

	// Preload the schema.
	loadSchemasFromEnvVar()

	// Start multiple goroutines that call Validate concurrently.
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			instance := map[string]interface{}{"id": id}
			err := Validate("concurrent_test", instance)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all goroutines to finish.
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestLoadSchemas_EmptyPath(t *testing.T) {
	defer cleanupEnv(t)
	loadSchemas([]string{""})
}

func TestLoadSchemas_NonExistentPath(t *testing.T) {
	defer cleanupEnv(t)
	loadSchemas([]string{"/non/existent/path/schema.json"})
}

func TestLoadSchemas_Directory(t *testing.T) {
	defer cleanupEnv(t)

	tmpDir := t.TempDir()

	schema1 := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "dir_test_1",
		"type": "string"
	}`
	schema2 := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "dir_test_2",
		"type": "integer"
	}`

	file1 := filepath.Join(tmpDir, "schema1.json")
	file2 := filepath.Join(tmpDir, "schema2.json")

	err := os.WriteFile(file1, []byte(schema1), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(file2, []byte(schema2), 0o644)
	require.NoError(t, err)

	loadSchemas([]string{tmpDir})

	schemaMu.RLock()
	_, exists1 := schemas["dir_test_1"]
	_, exists2 := schemas["dir_test_2"]
	schemaMu.RUnlock()
	assert.True(t, exists1)
	assert.True(t, exists2)
}

func TestLoadSchemas_NonJSONFile(t *testing.T) {
	defer cleanupEnv(t)

	tmpDir := t.TempDir()
	txtFile := filepath.Join(tmpDir, "not_a_schema.txt")
	err := os.WriteFile(txtFile, []byte("this is not json"), 0o644)
	require.NoError(t, err)

	loadSchemas([]string{tmpDir})

	schemaMu.RLock()
	count := len(schemas)
	schemaMu.RUnlock()
	assert.Equal(t, 0, count)
}

func TestLoadSchemasFromEnvVar_Unset(t *testing.T) {
	defer cleanupEnv(t)

	os.Unsetenv(RayRuntimeEnvPluginSchemasEnvVar)

	loadSchemasFromEnvVar()
}

func TestLoadSingleSchema_EmptyTitle(t *testing.T) {
	defer cleanupEnv(t)

	emptyTitleSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "",
		"type": "string"
	}`
	tmpFile := createTempSchemaFile(t, emptyTitleSchema)

	err := loadSingleSchema(tmpFile)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid title")
}

func TestLoadDefaultSchemas(t *testing.T) {
	defer cleanupEnv(t)

	loadDefaultSchemas()
}

func TestValidate_ArrayInstance(t *testing.T) {
	defer cleanupEnv(t)

	schemaContent := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "array_test",
		"type": "array",
		"items": {"type": "string"}
	}`
	tmpFile := createTempSchemaFile(t, schemaContent)
	os.Setenv(RayRuntimeEnvPluginSchemasEnvVar, tmpFile)
	loadSchemasFromEnvVar()

	validInstance := []interface{}{"a", "b", "c"}
	err := Validate("array_test", validInstance)
	assert.NoError(t, err)

	invalidInstance := []interface{}{"a", 42, "c"}
	err = Validate("array_test", invalidInstance)
	assert.Error(t, err)
}

func TestLoadSchemas_MixedPaths(t *testing.T) {
	defer cleanupEnv(t)

	tmpDir := t.TempDir()

	dirSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "mixed_dir_test",
		"type": "string"
	}`
	dirFile := filepath.Join(tmpDir, "dir_schema.json")
	err := os.WriteFile(dirFile, []byte(dirSchema), 0o644)
	require.NoError(t, err)

	singleSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "mixed_single_test",
		"type": "integer"
	}`
	singleFile := createTempSchemaFile(t, singleSchema)

	loadSchemas([]string{tmpDir, singleFile})

	schemaMu.RLock()
	_, exists1 := schemas["mixed_dir_test"]
	_, exists2 := schemas["mixed_single_test"]
	schemaMu.RUnlock()
	assert.True(t, exists1)
	assert.True(t, exists2)
}
