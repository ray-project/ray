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

package log

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// resolveFilename confines a client-supplied filename to rootLogDir, mirroring
// LogAgentV1Grpc._resolve_filename in log_agent.py:300-334. The candidate path is made
// absolute and cleaned without following symlinks first, so relative paths may
// legitimately traverse symlinks that point outside rootLogDir; only after the
// containment check does the path get fully resolved (symlinks followed).
func resolveFilename(rootLogDir, filename string) (string, error) {
	candidate, err := resolveLogPath(rootLogDir, filename)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(candidate)
	if err != nil {
		return "", fmt.Errorf("A file is not found at: %s", candidate)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("A file is not found at: %s", candidate)
	}
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve file %s: %w", candidate, err)
	}
	return resolved, nil
}

// resolveLogPath makes filename absolute relative to rootLogDir and verifies
// the result stays inside rootLogDir. Unlike resolveFilename it accepts
// directories so it can back the static /logs file browser. Symlinks are not
// followed here; the caller decides whether existence checks apply.
func resolveLogPath(rootLogDir, filename string) (string, error) {
	base, err := filepath.Abs(rootLogDir)
	if err != nil {
		return "", fmt.Errorf("resolve log dir %s: %w", rootLogDir, err)
	}
	var candidate string
	if filepath.IsAbs(filename) {
		candidate = filename
	} else {
		candidate = filepath.Join(base, filename)
	}
	if cleared, err := filepath.Abs(candidate); err == nil {
		candidate = cleared
	}
	if !inside(base, candidate) {
		return "", fmt.Errorf("%s not in %s", candidate, base)
	}
	return candidate, nil
}

// inside reports whether path is lexically contained in base after cleaning.
func inside(base, path string) bool {
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && rel != ""
}

// safeGlobFilter reports whether a ListLogs glob filter is a relative path
// without a ".." segment, mirroring the anchor and ".." checks
// LogAgentV1Grpc.ListLogs performs before globbing (log_agent.py:296-303).
func safeGlobFilter(filter string) bool {
	if filepath.IsAbs(filter) || strings.HasPrefix(filter, "/") {
		return false
	}
	for _, part := range strings.FieldsFunc(filter, func(r rune) bool {
		return r == '/' || r == '\\'
	}) {
		if part == ".." {
			return false
		}
	}
	return true
}

// globLogFiles returns the paths under logDir matching a pathlib-style glob.
// A "**" segment spans directories recursively, which filepath.Glob cannot
// express; this is what LogAgentV1Grpc.ListLogs globs with in Python. Absolute
// patterns are refused here as defense in depth; ListLogs rejects unsafe
// filters up front with InvalidArgument.
func globLogFiles(logDir, pattern string) ([]string, error) {
	if filepath.IsAbs(pattern) {
		return nil, fmt.Errorf("Non-relative patterns are unsupported")
	}
	re, err := globToRegex(pattern)
	if err != nil {
		return nil, err
	}
	var matches []string
	walkErr := filepath.WalkDir(logDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == logDir {
			return nil
		}
		rel, rerr := filepath.Rel(logDir, p)
		if rerr != nil {
			return nil
		}
		if re.MatchString(filepath.ToSlash(rel)) {
			matches = append(matches, p)
		}
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	sort.Strings(matches)
	return matches, nil
}

// globToRegex translates a "/"-separated glob into a regular expression.
// A "**" segment matches across directory levels, including zero levels when
// it is followed by more segments.
func globToRegex(pattern string) (*regexp.Regexp, error) {
	segments := strings.Split(pattern, "/")
	var b strings.Builder
	b.WriteString("^")
	for i, seg := range segments {
		if i > 0 && segments[i-1] != "**" {
			b.WriteString("/")
		}
		if seg == "**" {
			if i == len(segments)-1 {
				b.WriteString(".*")
			} else {
				b.WriteString("(?:.*/)?")
			}
			continue
		}
		b.WriteString(fnmatchSegmentToRegex(seg))
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}

// fnmatchSegmentToRegex translates one glob segment with fnmatch's character
// class and wildcard semantics; "*" and "?" never match "/".
func fnmatchSegmentToRegex(seg string) string {
	var b strings.Builder
	for i := 0; i < len(seg); i++ {
		c := seg[i]
		switch c {
		case '*':
			b.WriteString("[^/]*")
		case '?':
			b.WriteString("[^/]")
		case '[':
			j := i + 1
			if j < len(seg) && (seg[j] == '!' || seg[j] == '^') {
				j++
			}
			if j < len(seg) && seg[j] == ']' {
				j++
			}
			start := j
			for j < len(seg) && seg[j] != ']' {
				j++
			}
			if j >= len(seg) {
				// fnmatch treats an unterminated "[" as a literal bracket.
				b.WriteString(regexp.QuoteMeta("["))
				continue
			}
			cls := seg[start:j]
			if strings.HasPrefix(cls, "!") {
				cls = "^" + cls[1:]
			}
			b.WriteString("[" + cls + "]")
			i = j
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	return b.String()
}
