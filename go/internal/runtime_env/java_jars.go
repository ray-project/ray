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
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
)

// JavaJarsPlugin is the Java JAR files plugin.
// It implements the RuntimeEnvPlugin interface to support specifying Java JAR
// files in the runtime environment.
type JavaJarsPlugin struct {
	// resourcesDir is the resources directory.
	resourcesDir string
	// gcsClient is the GCS client.
	gcsClient gcs.Client
}

// Name returns the plugin name.
func (p *JavaJarsPlugin) Name() string {
	return FieldJavaJars
}

// Priority returns the plugin priority.
// The java_jars plugin uses priority 10, staggered from working_dir (5).
func (p *JavaJarsPlugin) Priority() int {
	return 10
}

// toStringSlice converts an interface{} value to a []string.
// Both []interface{} and []string are accepted. For []interface{}, every
// element is validated to be a string.
func toStringSlice(val interface{}) ([]string, error) {
	if val == nil {
		return nil, nil
	}

	switch v := val.(type) {
	case []interface{}:
		result := make([]string, 0, len(v))
		for i, item := range v {
			str, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("element [%d] must be a string, got %T", i, item)
			}
			result = append(result, str)
		}
		return result, nil
	case []string:
		return v, nil
	default:
		return nil, fmt.Errorf("must be an array of strings, got %T", val)
	}
}

// Validate validates the runtime environment configuration.
// It checks that the java_jars field is an array of strings.
func (p *JavaJarsPlugin) Validate(runtimeEnv *RuntimeEnv) error {
	javaJarsVal, ok := (*runtimeEnv)[FieldJavaJars]
	if !ok || javaJarsVal == nil {
		return nil // No java_jars field, nothing to validate.
	}

	// Validate using the shared type-conversion helper.
	_, err := toStringSlice(javaJarsVal)
	return err
}

// GetURIs returns the list of URIs for the Java JAR files.
func (p *JavaJarsPlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	javaJarsVal, ok := (*runtimeEnv)[FieldJavaJars]
	if !ok || javaJarsVal == nil {
		return []string{}
	}

	// Use the shared type-conversion helper; errors are ignored because
	// Validate has already run.
	uris, _ := toStringSlice(javaJarsVal)
	if uris == nil {
		return []string{}
	}

	return uris
}

// Create creates and installs the Java JAR files.
// It downloads and unpacks the JAR files into the resources directory and
// returns the disk space consumed (in bytes).
func (p *JavaJarsPlugin) Create(
	ctx context.Context,
	uri string,
	runtimeEnv *RuntimeEnv,
	context *RuntimeEnvContext,
) (int64, error) {

	var localPath string

	// Check whether the URI is a local path.
	if common.IsPath(uri) {
		// Local path, use it directly.
		localPath = filepath.Clean(uri)
		info, err := os.Stat(localPath)
		if err != nil {
			return 0, fmt.Errorf("local JAR file %s does not exist: %w", localPath, err)
		}
		if info.IsDir() {
			return 0, fmt.Errorf("%s is a directory, expected a JAR file", localPath)
		}
	} else {
		// Remote URI, parse and download it.
		protocol, _, err := ParseURI(uri)
		if err != nil {
			return 0, fmt.Errorf("invalid URI %s: %w", uri, err)
		}

		if protocol == ProtocolGCS || IsRemoteProtocol(protocol) {
			// Remote URI, download it.
			localPath, err = DownloadAndUnpackPackage(
				ctx,
				uri,
				p.resourcesDir,
				p.gcsClient,
				true,
			)
			if err != nil {
				return 0, fmt.Errorf("failed to download JAR %s: %w", uri, err)
			}
		} else {
			return 0, fmt.Errorf("unsupported protocol %s for JAR %s", protocol, uri)
		}
	}

	// Compute the file size.
	var size int64
	info, err := os.Stat(localPath)
	if err != nil {
		return 0, err
	}
	if info.IsDir() {
		size, err = common.DirSizeBytes(localPath)
	} else {
		size = info.Size()
	}
	if err != nil {
		return 0, err
	}

	return size, nil
}

// ModifyContext modifies the context to set up the Java JAR files.
// It prepends the JAR file paths to the CLASSPATH environment variable.
func (p *JavaJarsPlugin) ModifyContext(
	uris []string,
	runtimeEnv *RuntimeEnv,
	context *RuntimeEnvContext,
) error {

	if len(uris) == 0 {
		return nil
	}

	var jarPaths []string
	for _, uri := range uris {
		localPath := getJarLocalPath(uri, p.resourcesDir)
		jarPaths = append(jarPaths, localPath)
	}

	classpath := strings.Join(jarPaths, string(os.PathListSeparator))

	if existingClasspath, ok := context.EnvVars[ClassPathEnvVar]; ok && existingClasspath != "" {
		classpath = classpath + string(os.PathListSeparator) + existingClasspath
	}

	if context.EnvVars == nil {
		context.EnvVars = make(map[string]string)
	}
	context.EnvVars[ClassPathEnvVar] = classpath

	log.Log.Info("Set CLASSPATH for Java JARs", "classpath", classpath)
	return nil
}

// DeleteURI deletes the Java JAR file for the given URI.
// It returns the amount of space freed (in bytes).
func (p *JavaJarsPlugin) DeleteURI(uri string) int64 {

	log.Log.Info("Received request to delete Java JAR URI", "uri", uri)

	// JAR files are not unpacked, so use the file path directly.
	localPath := getJarLocalPath(uri, p.resourcesDir)

	// Get the file size.
	size := int64(0)
	if info, err := os.Stat(localPath); err == nil {
		if info.IsDir() {
			dirSize, _ := common.DirSizeBytes(localPath)
			size = dirSize
		} else {
			size = info.Size()
		}
	}

	// Delete the package.
	deleted, err := DeletePackage(context.Background(), uri, p.resourcesDir)
	if err != nil {
		log.Log.Error(err, "Failed to delete Java JAR URI", "uri", uri)
		return 0
	}
	if !deleted {
		log.Log.V(1).Info("Tried to delete nonexistent URI", "uri", uri)
		return 0
	}

	return size
}

// getJarLocalPath returns the local path of a JAR file.
// Local paths are returned as-is; other URIs are resolved via _getLocalPath.
// A JAR file is a single file and is not unpacked into a directory.
func getJarLocalPath(uri string, resourcesDir string) string {
	if common.IsPath(uri) {
		return filepath.Clean(uri)
	}
	return _getLocalPath(resourcesDir, uri)
}

// needsRemoteDownload reports whether the URI must be downloaded from a remote
// source. It returns true for remote URIs and false for local files.
func needsRemoteDownload(uri string) bool {
	if common.IsPath(uri) {
		return false
	}

	parsed, err := url.Parse(uri)
	if err != nil {
		return false
	}

	protocol := Protocol(parsed.Scheme)

	return protocol == ProtocolGCS || protocol == "http" || (IsRemoteProtocol(protocol) && protocol != ProtocolFile)
}

// NewJavaJarsPlugin creates a new Java JARs plugin instance.
func NewJavaJarsPlugin(resourcesDir string, gcsClient gcs.Client) (*JavaJarsPlugin, error) {
	javaJarsResourcesDir, err := createResourcesSubdir(resourcesDir, "java_jars_files")
	if err != nil {
		return nil, fmt.Errorf("failed to create Java JARs resources directory: %w", err)
	}

	return &JavaJarsPlugin{
		resourcesDir: javaJarsResourcesDir,
		gcsClient:    gcsClient,
	}, nil
}
