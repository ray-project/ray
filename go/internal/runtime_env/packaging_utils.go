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
	"crypto/sha1"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
	gitignore "github.com/sabhiram/go-gitignore"
)

// _mibString converts a byte count into its MiB string representation.
func _mibString(numBytes int64) string {
	sizeMiB := float64(numBytes) / float64(1024*1024)
	return fmt.Sprintf("%.2fMiB", sizeMiB)
}

// _toExtendedLengthPath converts a path to the extended-length form on Windows.
// On other platforms the path is returned unchanged.
func _toExtendedLengthPath(path string) string {
	if runtime.GOOS != "windows" {
		return path
	}

	// Make the path absolute and clean it.
	absPath, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	absPath = filepath.Clean(absPath)

	// Extended-length path prefix.
	extendedPrefix := `\\?\`

	// Already in extended-length form.
	if strings.HasPrefix(absPath, extendedPrefix) {
		return absPath
	}

	// UNC paths: \\\\server\\share -> \\\\?\\UNC\\server\\share.
	if strings.HasPrefix(absPath, `\\`) {
		return extendedPrefix + "UNC" + absPath[1:]
	}

	// Local paths: C:\path -> \\\\?\\C:\path.
	return extendedPrefix + absPath
}

// _xorBytes XORs two byte slices together.
func _xorBytes(left, right []byte) []byte {
	if len(left) > 0 && len(right) > 0 {
		minLen := len(left)
		if len(right) < minLen {
			minLen = len(right)
		}
		result := make([]byte, minLen)
		for i := 0; i < minLen; i++ {
			result[i] = left[i] ^ right[i]
		}
		return result
	}
	if len(left) > 0 {
		return left
	}
	return right
}

// _dirTravel walks a directory recursively, calling handler for every subpath.
func _dirTravel(
	path string,
	excludes []func(string) bool,
	handler func(string) error,
	includeGitignore bool,
) error {
	// Load the ignore rules for the current path.
	newExcludes, err := getExcludesFromIgnoreFiles(path, includeGitignore)
	if err != nil {
		return err
	}

	// Remember the length before appending so it can be unwound later.
	oldLen := len(excludes)
	excludes = append(excludes, newExcludes...)

	// Check whether this path should be skipped.
	skip := false
	for _, e := range excludes {
		if e(path) {
			skip = true
			break
		}
	}

	if !skip {
		if err := handler(path); err != nil {
			log.Log.Error(err, "Issue with path", "path", path)
			return err
		}

		// Recurse into subdirectories.
		info, err := os.Stat(path)
		if err == nil && info.IsDir() {
			entries, err := os.ReadDir(path)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				subPath := filepath.Join(path, entry.Name())
				if err := _dirTravel(subPath, excludes, handler, includeGitignore); err != nil {
					return err
				}
			}
		}
	}

	// Remove the exclude rules added at this level (backtracking).
	excludes = excludes[:oldLen]

	return nil
}

// _hashFileContentOrDirectoryName hashes a file's contents or a directory name.
func _hashFileContentOrDirectoryName(filepathStr string, relativePath string) ([]byte, error) {
	sha1Hash := sha1.New()

	// Hash the relative path.
	relPath, err := filepath.Rel(relativePath, filepathStr)
	if err != nil {
		relPath = filepathStr
	}
	sha1Hash.Write([]byte(relPath))

	info, err := os.Stat(filepathStr)
	if err != nil {
		return nil, err
	}

	// For files, also hash the contents.
	if !info.IsDir() {
		file, err := os.Open(filepathStr)
		if err != nil {
			log.Log.V(1).Info("Skipping contents of file when calculating package hash",
				"file", filepathStr, "error", err)
			return sha1Hash.Sum(nil), nil
		}
		defer file.Close()

		bufSize := 4096 * 1024
		buf := make([]byte, bufSize)
		for {
			n, err := file.Read(buf)
			if n > 0 {
				sha1Hash.Write(buf[:n])
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
		}
	}

	return sha1Hash.Sum(nil), nil
}

// _hashFile computes the hash of a single file.
func _hashFile(filepathStr string, relativePath string) ([]byte, error) {
	fileHash, err := _hashFileContentOrDirectoryName(filepathStr, relativePath)
	if err != nil {
		return nil, err
	}

	// XOR with 8 bytes of "0".
	zeros := []byte("00000000")
	return _xorBytes(fileHash, zeros), nil
}

// _hashDirectory computes the hash of a directory.
func _hashDirectory(
	root string,
	relativePath string,
	excludeFunc func(string) bool,
	includeGitignore bool,
) ([]byte, error) {
	hashVal := []byte("00000000") // Start with 8 bytes of "0".

	var mu sync.Mutex

	handler := func(path string) error {
		fileHash, err := _hashFileContentOrDirectoryName(path, relativePath)
		if err != nil {
			return err
		}
		mu.Lock()
		hashVal = _xorBytes(hashVal, fileHash)
		mu.Unlock()
		return nil
	}

	var excludes []func(string) bool
	if excludeFunc != nil {
		excludes = []func(string) bool{excludeFunc}
	}

	err := _dirTravel(root, excludes, handler, includeGitignore)
	if err != nil {
		return nil, err
	}

	return hashVal, nil
}

// parsePath parses a path and validates it.
func parsePath(pkgPath string) error {
	path := filepath.Clean(pkgPath)
	_, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s is not a valid path: %w", path, err)
	}
	return nil
}

// ParseURI splits a package URI into protocol and package name.
func ParseURI(pkgURI string) (Protocol, string, error) {
	if common.IsPath(pkgURI) {
		return "", "", fmt.Errorf("expected URI but received path %s", pkgURI)
	}

	parsed, err := url.Parse(pkgURI)
	if err != nil {
		return "", "", err
	}

	protocol := Protocol(parsed.Scheme)

	// Validate the protocol.
	if !isValidProtocol(protocol) {
		return "", "", fmt.Errorf("invalid protocol for runtime_env URI %q. Supported protocols: %v",
			pkgURI, GetProtocols())
	}

	var packageName string

	if IsRemoteProtocol(protocol) {
		if strings.HasSuffix(parsed.Path, ".whl") {
			// .whl file names are kept as-is.
			parts := strings.Split(parsed.Path, "/")
			packageName = parts[len(parts)-1]
		} else {
			packageName = fmt.Sprintf("%s_%s%s", protocol, parsed.Host, parsed.Path)

			// Replace disallowed characters.
			disallowedChars := []string{"/", ":", "@", "+", " ", "(", ")"}
			for _, char := range disallowedChars {
				packageName = strings.ReplaceAll(packageName, char, "_")
			}

			// Remove all periods except the last one.
			lastDotIndex := strings.LastIndex(packageName, ".")
			if lastDotIndex != -1 {
				packageName = strings.ReplaceAll(packageName[:lastDotIndex], ".", "_") + packageName[lastDotIndex:]
			}
		}
	} else {
		packageName = parsed.Host
	}

	return protocol, packageName, nil
}

// IsZipURI reports whether the URI refers to a zip file.
func IsZipURI(uri string) bool {
	_, packageName, err := ParseURI(uri)
	if err != nil {
		return false
	}
	return strings.HasSuffix(packageName, ".zip")
}

// IsWhlURI reports whether the URI refers to a wheel file.
func IsWhlURI(uri string) bool {
	_, packageName, err := ParseURI(uri)
	if err != nil {
		return false
	}
	return strings.HasSuffix(packageName, ".whl")
}

// IsJarURI reports whether the URI refers to a jar file.
func IsJarURI(uri string) bool {
	return strings.HasSuffix(strings.ToLower(uri), ".jar")
}

// _getExcludes builds a matcher from an exclude list.
// The exclude patterns are parsed with the go-gitignore library.
func _getExcludes(path string, excludes []string) func(string) bool {
	absPath, _ := filepath.Abs(path)

	// Compile the exclude patterns with go-gitignore; gitwildmatch syntax (e.g. **/node_modules/) is supported.
	spec := gitignore.CompileIgnoreLines(excludes...)

	return func(p string) bool {
		absP, _ := filepath.Abs(p)
		relPath, err := filepath.Rel(absPath, absP)
		if err != nil {
			return false
		}
		// Match with the gitignore library's MatchesPath method.
		return spec.MatchesPath(relPath)
	}
}

// _getIgnoreFile builds a matcher from an ignore file.
func _getIgnoreFile(path string, ignoreFile string) (func(string) bool, error) {
	absPath, _ := filepath.Abs(path)
	ignoreFilePath := filepath.Join(absPath, ignoreFile)

	info, err := os.Stat(ignoreFilePath)
	if err != nil || info.IsDir() {
		return nil, nil // Missing file yields nil.
	}

	// Parse the ignore file with go-gitignore, matching Python's PathSpec("gitwildmatch") behavior.
	spec, err := gitignore.CompileIgnoreFile(ignoreFilePath)
	if err != nil {
		return nil, err
	}

	return func(p string) bool {
		absP, _ := filepath.Abs(p)
		relPath, err := filepath.Rel(absPath, absP)
		if err != nil {
			return false
		}
		// Match with the gitignore library's Matches method.
		return spec.MatchesPath(relPath)
	}, nil
}

// getExcludesFromIgnoreFiles builds matchers from the .gitignore and .rayignore files.
func getExcludesFromIgnoreFiles(path string, includeGitignore bool) ([]func(string) bool, error) {
	var toIgnore []func(string) bool
	var ignoreFiles []string

	if includeGitignore {
		g, err := _getIgnoreFile(path, ".gitignore")
		if err != nil {
			return nil, err
		}
		if g != nil {
			toIgnore = append(toIgnore, g)
			ignoreFiles = append(ignoreFiles, filepath.Join(path, ".gitignore"))
		}
	}

	r, err := _getIgnoreFile(path, ".rayignore")
	if err != nil {
		return nil, err
	}
	if r != nil {
		toIgnore = append(toIgnore, r)
		ignoreFiles = append(ignoreFiles, filepath.Join(path, ".rayignore"))
	}

	if len(ignoreFiles) > 0 {
		log.Log.Info("Ignoring upload to cluster for these files", "files", ignoreFiles)
	}

	return toIgnore, nil
}

// _getLocalPath derives the local path from a URI.
func _getLocalPath(baseDirectory, pkgURI string) string {
	_, pkgName, _ := ParseURI(pkgURI)
	return filepath.Join(baseDirectory, pkgName)
}

// GetLocalDirFromURI derives the local directory from a URI.
//
// Supported package formats and their directory names:
// - .zip files: /tmp/pkg.zip -> /tmp/pkg
// - .whl files: /tmp/pkg.whl -> /tmp/pkg
// - .jar files: /tmp/pkg.jar -> /tmp/pkg
//
// Ray runtime envs only support the single-extension formats above, not multi-extension
// ones like .tar.gz, so stripping the last extension with filepath.Ext() is sufficient.
func GetLocalDirFromURI(uri string, baseDirectory string) string {
	pkgFile := _getLocalPath(baseDirectory, uri)
	// Strip the single extension (.zip, .whl, .jar etc.).
	ext := filepath.Ext(pkgFile)
	localDir := strings.TrimSuffix(pkgFile, ext)
	return localDir
}

// GetTopLevelDirFromCompressedPackage returns the top-level directory of a compressed package.
func GetTopLevelDirFromCompressedPackage(packagePath string) (string, error) {
	reader, err := zip.OpenReader(packagePath)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	var topLevelDirectory string

	for _, file := range reader.File {
		if topLevelDirectory == "" {
			// Check whether this is a top-level file.
			if !strings.Contains(file.Name, "/") {
				return "", nil // A file exists at the top level.
			}
			// Get the top-level directory name.
			parts := strings.SplitN(file.Name, "/", 2)
			dirName := parts[0]
			if dirName == MAC_OS_ZIP_HIDDEN_DIR_NAME {
				continue
			}
			topLevelDirectory = dirName
		} else {
			// Confirm every entry belongs to the same top-level directory.
			if !strings.Contains(file.Name, "/") {
				return "", nil
			}
			parts := strings.SplitN(file.Name, "/", 2)
			baseDir := parts[0]
			if baseDir != topLevelDirectory && baseDir != MAC_OS_ZIP_HIDDEN_DIR_NAME {
				return "", nil
			}
		}
	}

	return topLevelDirectory, nil
}

// createResourcesSubdir creates a resources subdirectory.
// Plugins use it to create their own subdirectory under resourcesDir during initialization.
// subdirName is the subdirectory name (e.g. "java_jars_files", "working_dir_files").
// It returns the created full path and an error.
func createResourcesSubdir(resourcesDir string, subdirName string) (string, error) {
	resourcesSubdir := filepath.Join(resourcesDir, subdirName)
	if err := os.MkdirAll(resourcesSubdir, 0755); err != nil {
		log.Log.Error(err, "Failed to create resources subdirectory", "subdir", resourcesSubdir)
		return "", err
	}
	return resourcesSubdir, nil
}

// removeDirFromFilepaths removes a directory from its file paths.
func removeDirFromFilepaths(baseDir string, rdir string) error {
	// Create a temporary directory.
	tmpDir, err := os.MkdirTemp("", "ray_pkg_*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	// Apply the extended-length path form on Windows to handle long paths.
	extendedTmpDir := _toExtendedLengthPath(tmpDir)

	// Move rdir into the temporary directory.
	srcPath := filepath.Join(baseDir, rdir)
	dstPath := filepath.Join(extendedTmpDir, rdir)
	if err := os.Rename(srcPath, dstPath); err != nil {
		// Fall back to copy+remove across file systems.
		if err := common.CopyDir(srcPath, dstPath); err != nil {
			return err
		}
		os.RemoveAll(srcPath)
	}

	// Move the contents of rdir into baseDir.
	entries, err := os.ReadDir(dstPath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		src := filepath.Join(dstPath, entry.Name())
		dst := filepath.Join(baseDir, entry.Name())
		if err := os.Rename(src, dst); err != nil {
			if err := common.CopyAll(src, dst); err != nil {
				return err
			}
			os.RemoveAll(src)
		}
	}

	return nil
}
