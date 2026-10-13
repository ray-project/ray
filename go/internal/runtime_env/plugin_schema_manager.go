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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/pkg/log"
	"github.com/xeipuuv/gojsonschema"
)

const (
	// RayRuntimeEnvPluginSchemasEnvVar is the environment variable that points
	// to extra schema paths.
	RayRuntimeEnvPluginSchemasEnvVar = "RAY_RUNTIME_ENV_PLUGIN_SCHEMAS"
	// RayRuntimeEnvPluginSchemaSuffix is the schema file suffix.
	RayRuntimeEnvPluginSchemaSuffix = ".json"
)

var (
	// schemaMu guards concurrent access to schemas.
	schemaMu sync.RWMutex
	// schemas holds the loaded JSON schemas keyed by title.
	schemas = make(map[string]json.RawMessage)
	// loadMu serializes the one-time lazy load so concurrent first Validate
	// calls do not load (and race registering) the schemas twice. It is held
	// only while loading, never while validating.
	loadMu sync.Mutex
	// loaded marks whether the schemas have been loaded. It is read and written
	// under loadMu.
	loaded bool
	// defaultSchemaPath is the path of the default schema directory.
	defaultSchemaPath string
)

func init() {
	// Resolve the default schema directory. The JSON schemas ship alongside the
	// source (schemas/), so try the layouts that actually occur at runtime:
	//  1. next to the executable (an installed/built binary), where the
	//     schemas directory sits under internal/runtime_env either one level
	//     up (from a bin/ dir) or directly under the executable dir;
	//  2. relative to this source file, which covers `go test` and bazel test
	//     runs that execute from the package directory.
	// The first existing candidate wins; the historical ../../runtime_env/schemas
	// value resolved to a directory that never existed and is dropped.
	execPath, err := os.Executable()
	if err != nil {
		execPath = ""
	}
	execDir := filepath.Dir(execPath)
	_, currentFile, _, ok := runtime.Caller(0)
	sourceDir := ""
	if ok {
		sourceDir = filepath.Dir(currentFile)
	}

	candidates := make([]string, 0, 3)
	if execPath != "" {
		candidates = append(candidates,
			filepath.Join(execDir, "..", "internal", "runtime_env", "schemas"),
			filepath.Join(execDir, "internal", "runtime_env", "schemas"),
		)
	}
	if sourceDir != "" {
		candidates = append(candidates, filepath.Join(sourceDir, "schemas"))
	}

	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			defaultSchemaPath = candidate
			return
		}
	}
	// No candidate exists. Keep the last (source-relative) path so the load
	// error names a concrete location; loadSchemas skips a missing path with a
	// log line rather than failing hard.
	if len(candidates) > 0 {
		defaultSchemaPath = candidates[len(candidates)-1]
	}
}

// loadSchemas loads schema files from the given path list.
// schemaPaths: a list of file or directory paths.
// Loading is best-effort: every failure is logged per path and the walk
// continues, so there is no error to return.
func loadSchemas(schemaPaths []string) {
	for _, path := range schemaPaths {
		if path == "" {
			continue
		}

		fileInfo, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				log.Log.V(1).Info("Schema path does not exist, skipping", "path", path)
				continue
			}
			log.Log.Error(err, "Failed to stat schema path", "path", path)
			continue
		}

		if fileInfo.IsDir() {
			// Recursively walk the directory.
			err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
				if err != nil {
					log.Log.Error(err, "Error accessing file during walk", "path", filePath)
					return nil // Keep walking the remaining files.
				}

				if !info.IsDir() && strings.HasSuffix(info.Name(), RayRuntimeEnvPluginSchemaSuffix) {
					if err := loadSingleSchema(filePath); err != nil {
						log.Log.Error(err, "Failed to load schema from directory walk", "path", filePath)
					}
				}
				return nil
			})
			if err != nil {
				log.Log.Error(err, "Failed to walk schema directory", "path", path)
			}
		} else {
			// A single file.
			if strings.HasSuffix(path, RayRuntimeEnvPluginSchemaSuffix) {
				if err := loadSingleSchema(path); err != nil {
					log.Log.Error(err, "Failed to load single schema file", "path", path)
				}
			} else {
				log.Log.V(1).Info("Skipping non-JSON file", "path", path)
			}
		}
	}
}

// loadSingleSchema loads a single schema file.
// filePath: the schema file path.
// It returns an error if loading fails.
func loadSingleSchema(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read schema file %s: %w", filePath, err)
	}

	// Parse the JSON to validate it and to extract the title.
	var schemaMap map[string]interface{}
	if err := json.Unmarshal(data, &schemaMap); err != nil {
		return fmt.Errorf("failed to parse JSON in schema file %s: %w", filePath, err)
	}

	// Check the title field.
	titleRaw, ok := schemaMap["title"]
	if !ok {
		return fmt.Errorf("no valid title in runtime env schema %s", filePath)
	}

	title, ok := titleRaw.(string)
	if !ok || title == "" {
		return fmt.Errorf("invalid title in runtime env schema %s: must be a non-empty string", filePath)
	}

	// Check for a title conflict.
	schemaMu.Lock()
	defer schemaMu.Unlock()

	if _, exists := schemas[title]; exists {
		// Title conflict.
		return fmt.Errorf("the 'title' of runtime env schema %s conflicts with previously loaded schema for title '%s'", filePath, title)
	}

	// Store the schema (keeping the original JSON form).
	schemas[title] = json.RawMessage(data)
	log.Log.V(1).Info("Successfully loaded schema", "title", title, "path", filePath)
	return nil
}

// loadDefaultSchemas loads the schemas from the default directory.
func loadDefaultSchemas() {
	log.Log.V(1).Info("Loading schemas from default directory", "path", defaultSchemaPath)
	loadSchemas([]string{defaultSchemaPath})
}

// loadSchemasFromEnvVar loads schemas from the RAY_RUNTIME_ENV_PLUGIN_SCHEMAS
// environment variable.
func loadSchemasFromEnvVar() {
	envValue := os.Getenv(RayRuntimeEnvPluginSchemasEnvVar)
	if envValue == "" {
		return // The environment variable is not set, which is normal.
	}

	log.Log.Info("Loading schemas from environment variable", "env_var", RayRuntimeEnvPluginSchemasEnvVar, "value", envValue)

	// Parse the comma-separated path list.
	paths := strings.Split(envValue, ",")
	loadSchemas(paths)
}

// Validate validates a user-provided configuration instance against the named
// schema.
// name: the schema name (the title field of the JSON schema).
// instance: the configuration instance to validate (it can be a
// map[string]interface{}, a slice, or any other type).
// It returns an error if validation fails and nil on success.
func Validate(name string, instance interface{}) error {
	// Lazy load all schemas on the first call.
	loadMu.Lock()
	if !loaded {
		// Hold loadMu across the load: it is one-time and fast, and exclusive
		// loading prevents duplicate schema registration. schemaMu is taken
		// per-schema inside loadSingleSchema, so loadMu must stay a separate
		// lock to avoid a self-deadlock.
		loadDefaultSchemas()
		loadSchemasFromEnvVar()
		loaded = true
	}
	loadMu.Unlock()

	// Look up the schema.
	schemaMu.RLock()
	schemaJSON, exists := schemas[name]
	schemaMu.RUnlock()

	if !exists {
		// No matching schema was found, so skip validation (not an error).
		log.Log.V(1).Info("No matching schema found, skipping validation", "name", name)
		return nil
	}

	// Validate with gojsonschema.
	schemaLoader := gojsonschema.NewStringLoader(string(schemaJSON))
	documentLoader := gojsonschema.NewGoLoader(instance)

	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return fmt.Errorf("schema validation failed for '%s': %w", name, err)
	}

	if !result.Valid() {
		// Format the validation error messages.
		var errorMsgs []string
		for _, desc := range result.Errors() {
			errorMsgs = append(errorMsgs, desc.String())
		}
		return fmt.Errorf("validation failed for schema '%s': %s", name, strings.Join(errorMsgs, "; "))
	}

	log.Log.V(1).Info("Validation successful", "name", name)
	return nil
}

// Clear clears the loaded schemas and resets the loading state (mainly used by
// tests).
// Note: this function is not concurrency-safe; the caller must ensure that no
// Validate call is in flight.
func Clear() {
	schemaMu.Lock()
	schemas = make(map[string]json.RawMessage)
	schemaMu.Unlock()

	loadMu.Lock()
	loaded = false
	loadMu.Unlock()
	log.Log.V(1).Info("Cleared all loaded schemas")
}
