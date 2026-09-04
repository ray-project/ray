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


package common

import (
	"io"
	"os"
	"path/filepath"
)

// InterfaceToStringSlice converts an interface{} slice to a string slice.
// Non-string elements are converted to empty strings.
func InterfaceToStringSlice(v []interface{}) []string {
	if v == nil {
		return nil
	}
	result := make([]string, len(v))
	for i, item := range v {
		if s, ok := item.(string); ok {
			result[i] = s
		}
	}
	return result
}

// InterfaceMapToStringMap converts an interface{} map to a string map.
// Non-string values are skipped.
func InterfaceMapToStringMap(v map[string]interface{}) map[string]string {
	if v == nil {
		return nil
	}
	result := make(map[string]string)
	for k, val := range v {
		if s, ok := val.(string); ok {
			result[k] = s
		}
	}
	return result
}

// ConvertSlice converts an interface{} value to a []T.
// If val is already []T, it is returned as-is.
// If val is []interface{}, the converter is used.
// Otherwise, the default value is returned.
func ConvertSlice[T any](val interface{}, defaultValue []T, converter func([]interface{}) []T) []T {
	if arr, ok := val.([]T); ok {
		return arr
	}
	if arr, ok := val.([]interface{}); ok {
		return converter(arr)
	}
	return defaultValue
}

// ConvertMap converts an interface{} value to a map[string]T.
// If val is already map[string]T, it is returned as-is.
// If val is map[string]interface{}, the converter is used.
// Otherwise, the default value is returned.
func ConvertMap[T any](val interface{}, defaultValue map[string]T, converter func(map[string]interface{}) map[string]T) map[string]T {
	if m, ok := val.(map[string]T); ok {
		return m
	}
	if m, ok := val.(map[string]interface{}); ok {
		return converter(m)
	}
	return defaultValue
}

// CopyAll copies a file or directory.
// If src is a directory, the whole directory is copied recursively.
// If src is a file, a single file is copied.
func CopyAll(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}

	if info.IsDir() {
		return CopyDir(src, dst)
	}
	return CopyFile(src, dst)
}

// CopyFile copies a single file.
// It opens the source file, creates the destination file, and copies the
// content using io.Copy.
func CopyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// CopyDir recursively copies a directory.
// It creates the destination directory and walks every entry in the source,
// recursively copying each child item.
func CopyDir(src, dst string) error {
	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		if err := CopyAll(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

// DirSizeBytes computes the total size of a directory (in bytes).
func DirSizeBytes(dirPath string) (int64, error) {
	var totalSize int64 = 0

	err := filepath.WalkDir(dirPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			totalSize += info.Size()
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	return totalSize, nil
}

// DeduplicateStrings deduplicates a string slice while preserving the original
// order.
func DeduplicateStrings(items []string) []string {
	if items == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(items))
	result := make([]string, 0, len(items))
	for _, item := range items {
		if _, exists := seen[item]; !exists {
			seen[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}
