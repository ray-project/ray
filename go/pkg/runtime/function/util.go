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

package function

import (
	"fmt"
	"strings"
)

// ParseFunctionName parses a Go function name string into module, package, and function components.
// This is a shared utility function used by both the function registry and method extractor.
//
// Examples:
//   - "main.goAdd" -> moduleName="main", pkgPath="", funcNameOnly="goAdd"
//   - "github.com/example/app/pkg/tasks.MyFunc" -> moduleName="github.com/example/app", pkgPath="pkg/tasks", funcNameOnly="MyFunc"
//   - "github.com/example/app.(*MyType).MyMethod" -> moduleName="github.com/example/app", pkgPath="", funcNameOnly="MyType.MyMethod"
//
// Returns an error if the function name format is invalid (e.g., no '.' separator found).
func ParseFunctionName(fullName string) (moduleName, pkgPath, funcNameOnly string, err error) {
	// Handle method on type: "package.(*Type).Method" or "package.Type.Method"
	if idx := strings.Index(fullName, ".("); idx != -1 {
		// Method on pointer receiver: "package.(*Type).Method"
		pkgPart := fullName[:idx]
		// Extract type and method: "(*Type).Method"
		typeMethod := fullName[idx+1:] // "(*Type).Method"
		if strings.HasPrefix(typeMethod, "(") && strings.Contains(typeMethod, ").") {
			// Extract method name after ")."
			methodIdx := strings.Index(typeMethod, ").")
			funcNameOnly = typeMethod[methodIdx+2:] // "Method"
			// Type name is between "(" and ")."
			typeName := typeMethod[1:methodIdx] // "*Type"
			// Module and package from pkgPart
			moduleName, pkgPath = SplitModuleAndPackage(pkgPart)
			// For methods, use "TypeName.Method" as function name
			funcNameOnly = typeName + "." + funcNameOnly
			return moduleName, pkgPath, funcNameOnly, nil
		}
	}

	// For normal functions and methods, we need to find the last '.' that separates
	// the package path from the function name.
	// Key insight: module paths contain '/' but function names don't.
	// So we look for the last '.' that comes AFTER the last '/'.

	// Find the last '/' to locate the end of the module/package path
	lastSlash := strings.LastIndex(fullName, "/")

	// Find the last '.' in the string
	lastDot := strings.LastIndex(fullName, ".")

	if lastDot == -1 {
		// No '.' found - this shouldn't happen for normal Go functions
		return "", "", "", fmt.Errorf("invalid function name format: %s (no '.' found)", fullName)
	}

	// If lastDot <= lastSlash, it means all '.' are in the module path (e.g., "github.com")
	// and there's no function name separator. This is an edge case.
	if lastDot <= lastSlash {
		// The entire string is the module/package path, no function name
		// This shouldn't happen, but handle it gracefully
		moduleName, pkgPath = SplitModuleAndPackage(fullName)
		return moduleName, pkgPath, "", fmt.Errorf("no function name found in: %s", fullName)
	}

	// Everything before lastDot is the module/package path
	// Everything after lastDot is the function name
	pkgPart := fullName[:lastDot]
	funcNameOnly = fullName[lastDot+1:]

	// Check if this looks like a method (contains another '.' in pkgPart after the last '/')
	// Method format: "module/path.Type.Method" where Type.Method is after the last '/'
	afterSlash := pkgPart
	if lastSlash != -1 {
		afterSlash = pkgPart[lastSlash+1:]
	}

	if strings.Contains(afterSlash, ".") {
		// This is a method: pkgPart contains "Type" or "path.Type"
		// We need to extract the type name and method name
		// For now, treat it as a normal function and let SplitModuleAndPackage handle it
		// The function name will be "Type.Method"
	}

	moduleName, pkgPath = SplitModuleAndPackage(pkgPart)
	return moduleName, pkgPath, funcNameOnly, nil
}

// SplitModuleAndPackage splits a string like "github.com/example/app/pkg/tasks" into module and package.
// For "github.com/example/app/pkg/tasks":
//   - moduleName = "github.com/example/app"
//   - pkgPath = "pkg/tasks"
// For "main":
//   - moduleName = "main"
//   - pkgPath = ""
//
// The key insight is that module paths always contain a '.' in their first component
// (e.g., "github.com", "example.org", "internal"). We look for the first '/' that
// comes AFTER the first component with a '.' to find the module/package boundary.
//
// This is a shared utility function used by both the function registry and method extractor.
func SplitModuleAndPackage(s string) (moduleName, pkgPath string) {
	if s == "main" {
		return "main", ""
	}

	if s == "" {
		return "", ""
	}

	parts := strings.Split(s, "/")
	if len(parts) == 0 {
		return s, ""
	}

	// Find the module boundary.
	// Module paths typically follow the pattern: "domain.tld/org/repo"
	// For example: "github.com/ray-project/ray" where:
	//   - "github.com" is the domain
	//   - "ray-project" is the organization
	//   - "ray" is the repository
	// Package paths come after: "go/userfuncs"

	// Heuristic: module path is the first 3 components if the first contains '.'
	// This handles common patterns like:
	//   - "github.com/user/repo" -> module="github.com/user/repo", pkg=""
	//   - "github.com/user/repo/pkg/path" -> module="github.com/user/repo", pkg="pkg/path"

	if len(parts) == 0 {
		return s, ""
	}

	if strings.Contains(parts[0], ".") {
		// Domain-like first component (e.g., "github.com")
		// Module is typically the first 3 components: domain, org, repo
		moduleEnd := 3
		if moduleEnd > len(parts) {
			moduleEnd = len(parts)
		}

		// Only reduce moduleEnd if we have more than 3 components
		// (i.e., there are package path components after the module)
		// For exactly 3 components, assume all are module (no package)
		if moduleEnd == 3 && len(parts) > 3 {
			// We have extra components after the module, those are package path
			moduleName = strings.Join(parts[:3], "/")
			pkgPath = strings.Join(parts[3:], "/")
			return moduleName, pkgPath
		}

		// For 1-3 components, all are module
		// For more than 3 but moduleEnd was capped, use the cap
		moduleName = strings.Join(parts[:moduleEnd], "/")
		return moduleName, ""
	}

	// No domain-like component, use all as module
	return s, ""
}
