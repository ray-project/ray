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
	"runtime"
	"testing"
)

func TestIsPath_PosixPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping POSIX test on Windows")
	}

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "absolute_path",
			input:    "/home/user/file.txt",
			expected: true,
		},
		{
			name:     "relative_path",
			input:    "./file.txt",
			expected: true,
		},
		{
			name:     "current_directory",
			input:    ".",
			expected: true,
		},
		{
			name:     "parent_directory",
			input:    "..",
			expected: true,
		},
		{
			name:     "empty_string",
			input:    "",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPath(tt.input)
			if result != tt.expected {
				t.Errorf("IsPath(%q) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsPath_Uri(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping URI test on Windows")
	}

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "http_uri",
			input:    "http://example.com/file.txt",
			expected: false,
		},
		{
			name:     "https_uri",
			input:    "https://example.com/file.txt",
			expected: false,
		},
		{
			name:     "s3_uri",
			input:    "s3://bucket/file.txt",
			expected: false,
		},
		{
			name:     "gs_uri",
			input:    "gs://bucket/file.txt",
			expected: false,
		},
		{
			name:     "hdfs_uri",
			input:    "hdfs://namenode:9000/path",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPath(tt.input)
			if result != tt.expected {
				t.Errorf("IsPath(%q) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsPath_WindowsPath_DriveLetter(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Skipping Windows drive letter test on non-Windows")
	}

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "windows_absolute_path_backslash",
			input:    `C:\Users\user\file.txt`,
			expected: true,
		},
		{
			name:     "windows_absolute_path_forwardslash",
			input:    `C:/Users/user/file.txt`,
			expected: true,
		},
		{
			name:     "windows_drive_only",
			input:    `C:`,
			expected: true,
		},
		{
			name:     "windows_drive_with_colon_lowercase",
			input:    `c:\path\to\file`,
			expected: true,
		},
		{
			name:     "windows_uppercase_drive",
			input:    `D:\data\file.txt`,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPath(tt.input)
			if result != tt.expected {
				t.Errorf("IsPath(%q) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsPath_WindowsPath_NetworkAndRelative(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Skipping Windows network/relative path test on non-Windows")
	}

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "windows_unc_path",
			input:    `\\server\share\file.txt`,
			expected: true,
		},
		{
			name:     "windows_relative_path",
			input:    `.\file.txt`,
			expected: true,
		},
		{
			name:     "windows_parent_path",
			input:    `..\file.txt`,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPath(tt.input)
			if result != tt.expected {
				t.Errorf("IsPath(%q) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestGetWindowsDrive(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "valid_drive_uppercase",
			input:    `C:\path`,
			expected: "C:",
		},
		{
			name:     "valid_drive_lowercase",
			input:    `c:\path`,
			expected: "c:",
		},
		{
			name:     "valid_drive_forward_slash",
			input:    `C:/path`,
			expected: "C:",
		},
		{
			name:     "drive_only",
			input:    `C:`,
			expected: "C:",
		},
		{
			name:     "invalid_no_letter",
			input:    `1:\path`,
			expected: "",
		},
		{
			name:     "invalid_no_colon",
			input:    `C\path`,
			expected: "",
		},
		{
			name:     "invalid_empty",
			input:    "",
			expected: "",
		},
		{
			name:     "invalid_single_char",
			input:    "C",
			expected: "",
		},
		{
			name:     "invalid_third_char_not_separator",
			input:    `C:path`,
			expected: "",
		},
		{
			name:     "valid_z_drive",
			input:    `z:\data`,
			expected: "z:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getWindowsDrive(tt.input)
			if result != tt.expected {
				t.Errorf("getWindowsDrive(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsPath_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "file_scheme_uri",
			input:    "file:///home/user/file.txt",
			expected: false,
		},
		{
			name:     "path_with_query",
			input:    "/path/to/file?query=value",
			expected: true,
		},
		{
			name:     "path_with_fragment",
			input:    "/path/to/file#fragment",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPath(tt.input)
			if result != tt.expected {
				t.Errorf("IsPath(%q) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}
