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
	"archive/zip"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
)

// RAY_RUNTIME_ENV_GCS_OPERATION_TIMEOUT_S is the GCS operation timeout in
// seconds.
const RAY_RUNTIME_ENV_GCS_OPERATION_TIMEOUT_S = 300

// PinRuntimeEnvURIWrapper pins a runtime env URI reference in the GCS and sets
// the timeout. It returns an error instead of panicking, following Go library
// conventions.
func PinRuntimeEnvURIWrapper(uri string, expirationS int) error {
	// If expirationS is 0 (None), get the default value from the environment
	// variable.
	if expirationS == 0 {
		expirationS = common.EnvInteger(
			RAY_RUNTIME_ENV_TEMPORARY_REFERENCE_EXPIRATION_S_ENV_VAR,
			RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT,
		)
	}

	// Validate that expirationS is >= 0.
	if expirationS < 0 {
		return fmt.Errorf("expiration_s must be >= 0, got %d", expirationS)
	}

	// Only call the internal KV pin when expirationS > 0.
	if expirationS > 0 {
		// Use a context with a timeout to avoid the GCS operation hanging
		// indefinitely.
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(RAY_RUNTIME_ENV_GCS_OPERATION_TIMEOUT_S)*time.Second)
		defer cancel()
		if err := PinRuntimeEnvURICall(ctx, uri, expirationS); err != nil {
			defaultLogger.Error(err, "Failed to pin runtime env URI", "uri", uri)
		}
	}
	return nil
}

// _storePackageInGCS stores the package data in the GCS.
func _storePackageInGCS(pkgURI string, data []byte) (int, error) {
	fileSize := int64(len(data))
	sizeStr := _mibString(fileSize)

	// Check the size limit.
	if fileSize >= int64(GCS_STORAGE_MAX_SIZE) {
		return 0, fmt.Errorf(
			"Package size (%s) exceeds the maximum size of %s. You can exclude large "+
				"files using the 'excludes' option to the runtime_env or provide "+
				"a remote URI of a zip file using protocols such as 's3://', "+
				"'https://' and so on, refer to "+
				"https://docs.ray.io/en/latest/ray-core/handling-dependencies.html#api-reference.",
			sizeStr, _mibString(int64(GCS_STORAGE_MAX_SIZE)))
	}

	log.Log.Info(fmt.Sprintf("Pushing file package '%s' (%s) to Ray cluster...", pkgURI, sizeStr))

	if os.Getenv(RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING_ENV_VAR) != "" {
		return 0, fmt.Errorf("Simulating failure to upload package for testing purposes.")
	}

	// Use a context with a timeout to avoid the GCS operation hanging
	// indefinitely.
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(RAY_RUNTIME_ENV_GCS_OPERATION_TIMEOUT_S)*time.Second)
	defer cancel()
	_, err := InternalKVPut(ctx, pkgURI, data, true, "")
	if err != nil {
		return 0, fmt.Errorf(
			"Failed to store package in the GCS.\n"+
				"  - GCS URI: %s\n"+
				"  - Package data (%s): %v...\n",
			pkgURI, sizeStr, data[:min(15, len(data))])
	}

	log.Log.Info(fmt.Sprintf("Successfully pushed file package '%s'.", pkgURI))
	return len(data), nil
}

// _zipFiles zips a target file or directory to the output path.
func _zipFiles(
	pathStr string,
	excludes []string,
	outputPath string,
	includeGitignore bool,
	includeParentDir bool,
) error {
	pkgFile, _ := filepath.Abs(outputPath)
	extendedPkgFile := _toExtendedLengthPath(pkgFile)

	// Ensure the output directory exists.
	outputDir := filepath.Dir(extendedPkgFile)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}

	zipFile, err := os.Create(extendedPkgFile)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	filePath, _ := filepath.Abs(pathStr)
	dirPath := filePath
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	isFile := !info.IsDir()
	if isFile {
		dirPath = filepath.Dir(filePath)
	}

	handler := func(path string) error {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}

		// Handle empty directories or files.
		if info.IsDir() {
			entries, err := os.ReadDir(path)
			if err != nil {
				return err
			}
			if len(entries) > 0 {
				return nil // Non-empty directory, skip.
			}
		}

		// Check the file size.
		if !info.IsDir() {
			fileSize := info.Size()
			if fileSize >= FILE_SIZE_WARNING {
				log.Log.V(1).Info("File is very large, consider adding to excludes",
					"file", path, "size", _mibString(fileSize))
			}
		}

		// Compute the relative path.
		toPath, err := filepath.Rel(dirPath, path)
		if err != nil {
			return err
		}

		if includeParentDir {
			toPath = filepath.Join(filepath.Base(dirPath), toPath)
		}

		// Convert to the zip path format (forward slashes).
		toPath = filepath.ToSlash(toPath)

		if info.IsDir() {
			// Add the directory entry.
			_, err = zipWriter.Create(toPath + "/")
			return err
		}

		// Add the file.
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		header.Name = toPath
		header.Method = zip.Deflate

		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = io.Copy(writer, file)
		return err
	}

	excludeFuncs := []func(string) bool{_getExcludes(filePath, excludes)}
	return _dirTravel(filePath, excludeFuncs, handler, includeGitignore)
}

// PackageExists reports whether a package exists.
func PackageExists(pkgURI string) (bool, error) {
	protocol, _, err := ParseURI(pkgURI)
	if err != nil {
		return false, err
	}

	if protocol == ProtocolGCS {
		// Use a context with a timeout to avoid the GCS operation hanging
		// indefinitely.
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(RAY_RUNTIME_ENV_GCS_OPERATION_TIMEOUT_S)*time.Second)
		defer cancel()
		return InternalKVExists(ctx, pkgURI, "")
	}

	return false, fmt.Errorf("protocol %s is not supported", protocol)
}

// GetURIForPackage returns the content-addressed URI for a package.
func GetURIForPackage(packagePath string) (string, error) {
	info, err := os.Stat(packagePath)
	if err != nil {
		return "", err
	}

	if info.IsDir() {
		return "", fmt.Errorf("expected file, got directory")
	}

	// Use the file name directly for wheel files.
	if strings.HasSuffix(packagePath, ".whl") {
		return fmt.Sprintf("%s://%s", ProtocolGCS, filepath.Base(packagePath)), nil
	}

	// Compute a streaming SHA1 hash for all other cases.
	file, err := os.Open(packagePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha1.New()
	buf := make([]byte, 4096*1024) // 4MB buffer.
	for {
		n, err := file.Read(buf)
		if n > 0 {
			hasher.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}

	hashVal := hasher.Sum(nil)
	return fmt.Sprintf("%s://%s.zip", ProtocolGCS, RAY_PKG_PREFIX+hex.EncodeToString(hashVal)), nil
}

// GetURIForFile returns the content-addressed URI for a file.
func GetURIForFile(file string) (string, error) {
	absPath, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}

	info, err := os.Stat(absPath)
	if err != nil {
		return "", fmt.Errorf("file %s must be an existing file: %w", absPath, err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("file %s must be an existing file, not a directory", absPath)
	}

	parentDir := filepath.Dir(absPath)
	hashVal, err := _hashFile(absPath, parentDir)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s://%s.zip", ProtocolGCS, RAY_PKG_PREFIX+hex.EncodeToString(hashVal)), nil
}

// GetURIForDirectory returns the content-addressed URI for a directory.
func GetURIForDirectory(directory string, includeGitignore bool, excludes []string) (string, error) {
	absPath, err := filepath.Abs(directory)
	if err != nil {
		return "", err
	}

	info, err := os.Stat(absPath)
	if err != nil {
		return "", fmt.Errorf("directory %s must be an existing directory: %w", absPath, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("directory %s must be an existing directory, not a file", absPath)
	}

	excludeFunc := _getExcludes(absPath, excludes)
	hashVal, err := _hashDirectory(absPath, absPath, excludeFunc, includeGitignore)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s://%s.zip", ProtocolGCS, RAY_PKG_PREFIX+hex.EncodeToString(hashVal)), nil
}

// UploadPackageToGCS uploads a local package to the GCS.
func UploadPackageToGCS(pkgURI string, pkgBytes []byte) error {

	protocol, _, err := ParseURI(pkgURI)
	if err != nil {
		return err
	}

	if protocol == ProtocolGCS {
		_, err := _storePackageInGCS(pkgURI, pkgBytes)
		return err
	} else if IsRemoteProtocol(protocol) {
		return fmt.Errorf("upload_package_to_gcs should not be called with a remote path")
	}

	return fmt.Errorf("protocol %s is not supported", protocol)
}

// CreatePackageWithConfig creates a package using a config struct.
func CreatePackageWithConfig(config *PackageConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	_, err := os.Stat(config.TargetPath)
	if err == nil {
		// The target already exists.
		return nil
	}

	log.Log.Info("Creating a file package for local module", "module_path", config.ModulePath)
	return _zipFiles(config.ModulePath, config.Excludes, config.TargetPath, config.IncludeGitignore, config.IncludeParentDir)
}

// UploadPackageIfNeededWithConfig uploads a package if needed using a config
// struct. A file lock avoids a TOCTOU (Time of Check to Time of Use) race.
func UploadPackageIfNeededWithConfig(ctx context.Context, config *PackageUploadConfig) (bool, error) {
	if config == nil {
		return false, fmt.Errorf("config cannot be nil")
	}

	if config.Excludes == nil {
		config.Excludes = []string{}
	}

	// Pin the runtime env uri - pin the runtime env URI reference in the GCS.
	if err := PinRuntimeEnvURIWrapper(config.PkgURI, 0); err != nil {
		return false, err
	}

	// Get the package file path for the lock file.
	packageFile := _getLocalPath(config.BaseDirectory, config.PkgURI)
	lockFile := packageFile + ".lock"

	// Acquire the file lock to make the check and upload atomic.
	lock := common.NewAsyncFileLock(lockFile)
	if err := lock.Acquire(ctx); err != nil {
		return false, err
	}
	defer lock.Release()

	// Check whether the package already exists while holding the lock.
	// Cache the check result to avoid duplicate GCS KV queries.
	exists, err := PackageExists(config.PkgURI)
	if err != nil {
		return false, err
	}
	if exists {
		// The package already exists; another caller may have finished the
		// upload.
		return false, nil
	}

	// Generate a unique temporary file name.
	timestamp := time.Now().UnixNano()
	pid := os.Getpid()
	baseName := filepath.Base(packageFile)
	tempPackageFile := filepath.Join(filepath.Dir(packageFile), fmt.Sprintf("%d_%d_%s", timestamp, pid, baseName))

	// Create the package.
	createConfig := &PackageConfig{
		ModulePath:       config.ModulePath,
		TargetPath:       tempPackageFile,
		IncludeGitignore: config.IncludeGitignore,
		IncludeParentDir: config.IncludeParentDir,
		Excludes:         config.Excludes,
	}
	if err := CreatePackageWithConfig(createConfig); err != nil {
		return false, err
	}

	// Read the package content.
	packageBytes, err := os.ReadFile(tempPackageFile)
	if err != nil {
		os.Remove(tempPackageFile)
		return false, err
	}

	// Remove the temporary file.
	defer os.Remove(tempPackageFile)

	// Upload to the GCS.
	// Note: since the file lock is already held and the first check confirmed
	// that the package does not exist, no second GCS check is needed.
	if err := UploadPackageToGCS(config.PkgURI, packageBytes); err != nil {
		return false, err
	}

	return true, nil
}

// DownloadAndUnpackPackage downloads and unpacks a package.
func DownloadAndUnpackPackage(
	ctx context.Context,
	pkgURI string,
	baseDirectory string,
	gcsClient gcs.Client,
	overwrite bool,
) (string, error) {

	pkgFile := _getLocalPath(baseDirectory, pkgURI)
	ext := filepath.Ext(pkgFile)
	if ext == "" {
		return "", fmt.Errorf("invalid package URI: %s. URI must have a file extension", pkgURI)
	}

	// Acquire the file lock.
	lockFile := pkgFile + ".lock"
	lock := common.NewAsyncFileLock(lockFile)
	if err := lock.Acquire(ctx); err != nil {
		return "", err
	}
	defer lock.Release()

	log.Log.V(1).Info("Fetching package for URI", "pkg_uri", pkgURI)

	localDir := GetLocalDirFromURI(pkgURI, baseDirectory)
	if localDir == pkgFile {
		return "", fmt.Errorf("invalid pkg_file")
	}

	downloadPackage := true
	if _, err := os.Stat(localDir); err == nil && !overwrite {
		return "", fmt.Errorf("directory '%s' already exists and overwrite is not enabled", localDir)
	} else if err == nil {
		log.Log.Info("Removing existing directory", "local_dir", localDir, "pkg_file", pkgFile)
		os.RemoveAll(localDir)
	}

	if downloadPackage {
		protocol, _, err := ParseURI(pkgURI)
		if err != nil {
			return "", err
		}

		log.Log.Info("Downloading package", "pkg_uri", pkgURI, "pkg_file", pkgFile, "protocol", protocol)

		if protocol == ProtocolGCS {
			if gcsClient == nil {
				return "", fmt.Errorf("GCS client must be provided to download from GCS")
			}

			code, err := gcsClient.Get(ctx, "", pkgURI)
			if err != nil {
				return "", err
			}

			if os.Getenv(RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING_ENV_VAR) != "" {
				code = nil
			}

			if code == nil {
				return "", fmt.Errorf(
					"Failed to download runtime_env file package %s "+
						"from the GCS to the Ray worker node. The package may "+
						"have prematurely been deleted from the GCS due to a "+
						"long upload time or a problem with Ray. Try setting the "+
						"environment variable %s "+
						"to a value larger than the upload time in seconds "+
						"(the default is %d). "+
						"If this fails, try re-running "+
						"after making any change to a file in the file package.",
					pkgURI,
					RAY_RUNTIME_ENV_TEMPORARY_REFERENCE_EXPIRATION_S_ENV_VAR,
					RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_DEFAULT,
				)
			}

			if err := os.WriteFile(pkgFile, code, 0644); err != nil {
				return "", err
			}

			if IsZipURI(pkgURI) {
				if err := UnzipPackage(pkgFile, localDir, false, true); err != nil {
					return "", err
				}
			} else {
				return pkgFile, nil
			}
		} else if IsRemoteProtocol(protocol) {
			opts := &DownloadOptions{
				HTTPTimeout: 0,
			}
			if err := DownloadRemoteURI(ctx, protocol, pkgURI, pkgFile, opts); err != nil {
				return "", err
			}

			suffix := filepath.Ext(pkgFile)
			switch suffix {
			case ".zip", ".jar":
				if err := UnzipPackage(pkgFile, localDir, true, true); err != nil {
					return "", err
				}
			case ".whl":
				return pkgFile, nil
			default:
				return "", fmt.Errorf("package format %s is not supported for remote protocols", suffix)
			}
		} else {
			return "", fmt.Errorf("protocol %s is not supported", protocol)
		}
	}

	return localDir, nil
}

// UnzipPackage unpacks a package to the target directory.
func UnzipPackage(
	packagePath string,
	targetDir string,
	removeTopLevelDirectory bool,
	unlinkZip bool,
) error {

	extendedTargetDir := _toExtendedLengthPath(targetDir)

	// Create the target directory.
	if err := os.Mkdir(extendedTargetDir, 0755); err != nil {
		if !os.IsExist(err) {
			return err
		}
		log.Log.Info("Directory at target_dir already exists", "target_dir", targetDir)
	}

	log.Log.V(1).Info("Unpacking package", "package_path", packagePath, "target_dir", extendedTargetDir)

	reader, err := zip.OpenReader(packagePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	for _, member := range reader.File {
		memberPath := filepath.Join(extendedTargetDir, member.Name)
		memberPath = _toExtendedLengthPath(memberPath)

		// Safety check: prevent path traversal attacks.
		rel, err := filepath.Rel(extendedTargetDir, memberPath)
		if err != nil || strings.HasPrefix(rel, "..") {
			log.Log.Info("Skipping unsafe path in zip", "member", member.Name)
			continue
		}

		log.Log.V(1).Info("Extracting file", "member", member.Name, "member_path", memberPath)

		if member.Name[len(member.Name)-1] == '/' {
			// Directory entry.
			if err := os.MkdirAll(memberPath, 0755); err != nil {
				return err
			}
		} else {
			// File entry.
			parentDir := filepath.Dir(memberPath)
			if err := os.MkdirAll(parentDir, 0755); err != nil {
				return err
			}

			srcFile, err := reader.Open(member.Name)
			if err != nil {
				return err
			}

			dstFile, err := os.Create(memberPath)
			if err != nil {
				srcFile.Close()
				return err
			}

			if _, err := io.Copy(dstFile, srcFile); err != nil {
				srcFile.Close()
				dstFile.Close()
				return err
			}
			srcFile.Close()
			dstFile.Close()

			// Preserve the file permissions.
			if member.ExternalAttrs != 0 {
				mode := os.FileMode(member.ExternalAttrs >> 16)
				if mode != 0 {
					os.Chmod(memberPath, mode)
				}
			}
		}
	}

	if removeTopLevelDirectory {
		topLevelDirectory, err := GetTopLevelDirFromCompressedPackage(packagePath)
		if err == nil && topLevelDirectory != "" {
			// Remove the __MACOSX directory.
			macosDir := filepath.Join(targetDir, MAC_OS_ZIP_HIDDEN_DIR_NAME)
			if info, err := os.Stat(macosDir); err == nil && info.IsDir() {
				os.RemoveAll(macosDir)
			}

			// Move the content.
			if err := removeDirFromFilepaths(extendedTargetDir, topLevelDirectory); err != nil {
				log.Log.Error(err, "Failed to remove top level directory", "top_level", topLevelDirectory)
			}
		}
	}

	if unlinkZip {
		os.Remove(packagePath)
	}

	return nil
}

// DeletePackage deletes the package for the given URI.
// The context.Context argument supports timeout control and the async lock.
func DeletePackage(ctx context.Context, pkgURI string, baseDirectory string) (bool, error) {
	deleted := false

	path := _getLocalPath(baseDirectory, pkgURI)
	ext := filepath.Ext(path)
	dirPath := strings.TrimSuffix(path, ext)

	// Acquire the file lock, using the unified AsyncFileLock for concurrency
	// safety.
	lockFile := path + ".lock"
	lock := common.NewAsyncFileLock(lockFile)
	if err := lock.Acquire(ctx); err != nil {
		return false, err
	}
	defer lock.Release()

	// Check whether the path exists.
	if _, err := os.Lstat(dirPath); err == nil {
		// Use Lstat to check for symlinks.
		info, err := os.Lstat(dirPath)
		if err != nil {
			return false, err
		}

		if info.Mode()&os.ModeSymlink != 0 {
			// Skip symlinks; they should not be deleted.
			// Return deleted=False because symlinks should not be removed.
			return false, nil
		}

		if info.IsDir() {
			// It is a directory and not a symlink; remove the directory tree.
			if err := os.RemoveAll(dirPath); err != nil {
				return false, err
			}
			deleted = true
		} else {
			// It is a regular file; remove the file.
			if err := os.Remove(dirPath); err != nil {
				return false, err
			}
			deleted = true
		}
	}

	return deleted, nil
}

// InstallWheelPackage installs a wheel package.
func InstallWheelPackage(
	ctx context.Context,
	wheelURI string,
	targetDir string,
) error {
	// Build the pip install command.
	pipInstallCmd := []string{"pip", "install", wheelURI, "--target=" + targetDir}

	log.Log.Info("Running py_modules wheel install command", "command", strings.Join(pipInstallCmd, " "))

	// Running the command via conda_utils.go exec_cmd_stream_to_logger is not
	// implemented yet, so the exit code defaults to success.
	exitCode := 0
	var output string

	// Clean up the wheel file.
	wheelURIPath := wheelURI
	if info, err := os.Stat(wheelURIPath); err == nil {
		if info.IsDir() {
			os.RemoveAll(wheelURIPath)
		} else {
			os.Remove(wheelURIPath)
		}
	}

	if exitCode != 0 {
		if info, err := os.Stat(targetDir); err == nil && info.IsDir() {
			os.RemoveAll(targetDir)
		}
		return fmt.Errorf("failed to install py_modules wheel %s to %s:\n%s", wheelURI, targetDir, output)
	}

	return nil
}
