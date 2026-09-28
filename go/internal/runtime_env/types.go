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

// Constants related to RuntimeEnv plugins.
const (
	// RayRuntimeEnvClassFieldName is the plugin class-name field name.
	RayRuntimeEnvClassFieldName = "class_name"
	// RayRuntimeEnvPriorityFieldName is the plugin priority field name.
	RayRuntimeEnvPriorityFieldName = "priority"
	// RayRuntimeEnvPluginsEnvVar is the environment variable that lists runtime env plugins.
	RayRuntimeEnvPluginsEnvVar = "RAY_RUNTIME_ENV_PLUGINS"
	// RayRuntimeEnvPluginMinPriority is the minimum plugin priority.
	RayRuntimeEnvPluginMinPriority = 0
	// RayRuntimeEnvPluginMaxPriority is the maximum plugin priority.
	RayRuntimeEnvPluginMaxPriority = 100
	// RayRuntimeEnvPluginDefaultPriority is the default plugin priority.
	RayRuntimeEnvPluginDefaultPriority = 10
	// DefaultResourcesDir is the default resources directory.
	DefaultResourcesDir = "/tmp/ray"
	// ClassPathEnvVar is the name of the Java CLASSPATH environment variable.
	ClassPathEnvVar = "CLASSPATH"
)

// RuntimeEnv field name constants, used to avoid string typos and provide type-safe field access.
const (
	FieldPyModules              = "py_modules"
	FieldPyExecutable           = "py_executable"
	FieldJavaJars               = "java_jars"
	FieldWorkingDir             = "working_dir"
	FieldConda                  = "conda"
	FieldPip                    = "pip"
	FieldUv                     = "uv"
	FieldContainer              = "container"
	FieldExcludes               = "excludes"
	FieldEnvVars                = "env_vars"
	FieldRayRelease             = "_ray_release"
	FieldRayCommit              = "_ray_commit"
	FieldInjectCurrentRay       = "_inject_current_ray"
	FieldConfig                 = "config"
	FieldWorkerProcessSetupHook = "worker_process_setup_hook"
	FieldNsight                 = "_nsight"
	FieldRocprofSys             = "_rocprof_sys"
	FieldImageURI               = "image_uri"
)

// RayCommit is the Ray commit hash.
// It can be overridden through the RAY_COMMIT environment variable.
// TODO: inject the current commit at build time.
var RayCommit = ""

// ImageURICompatibleKeys lists the fields compatible with image_uri.
var ImageURICompatibleKeys = []string{
	FieldImageURI,
	FieldConfig,
	FieldEnvVars,
}

// RuntimeEnv defines a runtime environment.
// The configuration is stored as a map so it can be extended dynamically.
type RuntimeEnv map[string]interface{}

// KnownFields lists the known runtime env fields.
var KnownFields = []string{
	FieldPyModules,
	FieldPyExecutable,
	FieldJavaJars,
	FieldWorkingDir,
	FieldConda,
	FieldPip,
	FieldUv,
	FieldContainer,
	FieldExcludes,
	FieldEnvVars,
	FieldRayRelease,
	FieldRayCommit,
	FieldInjectCurrentRay,
	FieldConfig,
	FieldWorkerProcessSetupHook,
	FieldNsight,
	FieldRocprofSys,
	FieldImageURI,
}

// ExtensionFields lists the extension fields.
var ExtensionFields = []string{
	FieldRayRelease,
	FieldRayCommit,
	FieldInjectCurrentRay,
}

// ContainerConfig is the container configuration.
type ContainerConfig struct {
	Image      string   `json:"image,omitempty"`
	WorkerPath string   `json:"worker_path,omitempty"`
	RunOptions []string `json:"run_options,omitempty"`
}

// ModifyContextConfig holds the parameters for modifyContextShared.
type ModifyContextConfig struct {
	RayTmpDir  string
	ImageURI   string
	WorkerPath string
	RunOptions []string
	Context    *RuntimeEnvContext
}

// RuntimeEnvOptions holds the options for building a RuntimeEnv.
type RuntimeEnvOptions struct {
	PyModules              []string
	PyExecutable           string
	WorkingDir             string
	Pip                    interface{}
	Conda                  interface{}
	Container              *ContainerConfig
	EnvVars                map[string]string
	WorkerProcessSetupHook interface{}
	Nsight                 interface{}
	RocprofSys             interface{}
	Config                 RuntimeEnvConfig
	Validate               bool
	ImageURI               string
	Uv                     interface{}
	JavaJars               []string
	RayRelease             string
	RayCommit              string
	InjectCurrentRay       bool
	Excludes               []string
	ExtraFields            map[string]interface{}
}
