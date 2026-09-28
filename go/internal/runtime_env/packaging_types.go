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
	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
)

// FILE_SIZE_WARNING is the per-file size warning threshold (10MiB).
const FILE_SIZE_WARNING = 10 * 1024 * 1024

// RAY_PKG_PREFIX is the Ray package prefix.
const RAY_PKG_PREFIX = "_ray_pkg_"

// MAC_OS_ZIP_HIDDEN_DIR_NAME is the hidden directory name in macOS zip archives.
const MAC_OS_ZIP_HIDDEN_DIR_NAME = "__MACOSX"

// Keep in sync with max_grpc_message_size in ray_config_def.h.
var GCS_STORAGE_MAX_SIZE = common.EnvInteger(
	"RAY_max_grpc_message_size",
	common.GRPC_CPP_MAX_MESSAGE_SIZE,
)

// Environment variable names.
const (
	RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING_ENV_VAR          = "RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING"
	RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING_ENV_VAR        = "RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING"
	RAY_RUNTIME_ENV_TEMPORARY_REFERENCE_EXPIRATION_S_ENV_VAR = "RAY_RUNTIME_ENV_TEMPORARY_REFERENCE_EXPIRATION_S"
)

// RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT is the default URI pin expiration in seconds.
const RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT = 10 * 60

var defaultLogger = log.WithName("runtime-env-packaging")

// PackageConfig bundles the parameters for CreatePackage and UploadPackageIfNeeded.
type PackageConfig struct {
	// ModulePath is the module path (directory or file to package).
	ModulePath string
	// TargetPath is the target path (package file to create).
	TargetPath string
	// BaseDirectory is the base directory used to resolve relative paths.
	BaseDirectory string
	// IncludeGitignore reports whether gitignore rules apply.
	IncludeGitignore bool
	// IncludeParentDir reports whether the parent directory is included.
	IncludeParentDir bool
	// Excludes lists the files or directories to exclude.
	Excludes []string
}

// PackageUploadConfig bundles the parameters for UploadPackageIfNeeded.
type PackageUploadConfig struct {
	// PkgURI is the package URI.
	PkgURI string
	// BaseDirectory is the base directory.
	BaseDirectory string
	// ModulePath is the module path.
	ModulePath string
	// IncludeGitignore reports whether gitignore rules apply.
	IncludeGitignore bool
	// IncludeParentDir reports whether the parent directory is included.
	IncludeParentDir bool
	// Excludes lists the files or directories to exclude.
	Excludes []string
}
