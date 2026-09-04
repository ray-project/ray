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
	"net/url"
	"runtime"
	"strings"
)

// IsPath reports whether a string is a filesystem path rather than a URI.
//
// Returns true if the input is a path, otherwise false.
//
// Windows paths begin with a drive name, which urlparse would interpret as a
// URI scheme, so they must be treated differently from POSIX paths.
//
// For example, creating a directory returns a path like
// 'C:\Users\mp5n6ul72w\working_dir', whose scheme would be 'C:'.
func IsPath(pathOrURI string) bool {
	parsedURI, err := url.Parse(pathOrURI)
	if err != nil {
		return true
	}

	// Choose the path-type logic based on the runtime OS.
	if runtime.GOOS == "windows" {
		// Windows: follow the PureWindowsPath logic.
		drive := getWindowsDrive(pathOrURI)
		if drive != "" {
			// Drive path: the scheme must equal the drive letter (lower-cased),
			// e.g. "c" corresponds to "C:".
			// pathlib.PureWindowsPath("C:\\path").drive == "C:"
			// urlparse("C:\\path").scheme == "c"
			return strings.ToLower(parsedURI.Scheme) == strings.ToLower(strings.TrimSuffix(drive, ":"))
		}
		// Other Windows paths (containing backslashes): it is a path if there is no scheme.
		if strings.Contains(pathOrURI, `\`) {
			return parsedURI.Scheme == ""
		}
		// Otherwise: it is a path if there is no scheme.
		return parsedURI.Scheme == ""
	}

	// POSIX: follow the PurePosixPath logic - it is a path if there is no scheme.
	return parsedURI.Scheme == ""
}

// getWindowsDrive returns the drive of a Windows path (e.g. "C:").
// If the path is a valid Windows drive path (such as C:\ or C:/), it returns the
// drive letter plus a colon; otherwise it returns an empty string.
func getWindowsDrive(path string) string {
	if len(path) < 2 {
		return ""
	}
	c := path[0]
	isLetter := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
	if !isLetter || path[1] != ':' {
		return ""
	}
	// Check that the third character is a path separator (\ or /) or the end of the string.
	if len(path) == 2 || path[2] == '\\' || path[2] == '/' {
		return path[:2]
	}
	return ""
}
