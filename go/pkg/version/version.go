// Package version holds the Ray version and source commit shared across every
// Go component (raygo CLI and its subcommands), aligned with
// python/ray/_version.py. The single rayVersion/rayCommit source lives here so
// all components report identical values.
package version

// RayVersion and RayCommit are the single source of truth for the Ray version
// and source commit across the Go runtime, aligned with python/ray/_version.py.
// They are package-level variables (not constants) because a build injects the
// real values into the raygo binary at link time via
// `-ldflags -X` (go_binary x_defs). The defaults mirror the repository constants
// in python/ray/_version.py: the source commit is the {{RAY_COMMIT_SHA}}
// placeholder that a wheel build substitutes, the same way
// ci/build/build-manylinux-wheel.sh rewrites it during packaging.
var (
	RayVersion = "3.0.0.dev0"
	RayCommit  = "{{RAY_COMMIT_SHA}}"
)