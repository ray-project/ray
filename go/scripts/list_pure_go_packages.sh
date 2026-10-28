#!/usr/bin/env bash
# Print the Go packages of this module that compile and test under a plain Go
# toolchain (no bazel), one per line. A package is excluded, mechanically:
#   - when any of its non-test files use cgo (they link the C++ core
#     libraries, which only the bazel build provides), or it transitively
#     depends on such a package; standard-library cgo users such as net do
#     not count — a plain toolchain builds them;
#   - when any package in the transitive dependency closure of its test
#     binary fails to load — that closure covers the package's own imports
#     and the imports of its in-package and external test files, all
#     transitively — or when it has no non-test files at all (a test-only
#     package cannot be built by "go build" standalone). The generated
#     protobuf sources under go/proto exist only as bazel build outputs and
#     are absent from the source tree, so every package that transitively
#     imports them fails to load outside bazel; undeclared or build-tag-only
#     module imports fail the same way.
# The module graph is resolved against a throwaway copy of go.mod, so running
# this script never modifies the working tree.
set -euo pipefail
cd "$(dirname "$0")/.."

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT
cp go.mod "$tmpdir/go.mod"
cp go.sum "$tmpdir/go.sum" 2>/dev/null || true

# Resolve the module graph with -mod=mod against the copy; this fills in the
# missing requirements there instead of failing, and leaves the real go.mod
# untouched. -e keeps packages that fail to load visible in the output.
golist() {
  go list -mod=mod -modfile="$tmpdir/go.mod" -e "$@" ./...
}

# Every package in the dependency graph, marked "bad" when it fails to load
# or has non-test cgo files. Standard-library cgo users (net, os/user) are
# not marked: a plain toolchain builds them, so they disqualify nothing.
golist -deps -f '{{.ImportPath}}	{{if and (not .Standard) (or .Error (ne (len .CgoFiles) 0))}}bad{{end}}' \
  | awk -F'\t' '$2 == "bad" { print $1 }' > "$tmpdir/excluded"

# Keep the module's own packages that have non-test files, are not excluded
# themselves, and import no excluded package — neither from their non-test
# files nor from their test files — so "go build", "go vet" and "go test"
# all succeed. With -test, "go list" reports an extra "P.test" entry per
# tested package P whose Deps are the transitive closure of the whole test
# binary (the package, its in-package and external test imports, and
# everything those reach), which subsumes the direct TestImports/XTestImports
# check. The -test listing also carries pseudo entries — dotted "P.test"
# test binaries and bracketed "P_test [P.test]" external test packages — so
# only real module packages (module prefix, no space or bracket, no .test
# suffix) are printed.
golist -test -f '{{.ImportPath}}|{{if eq (len .GoFiles) 0}}notest|{{end}}{{join .Deps " "}}' > "$tmpdir/listed"
awk -F'|' -v excluded="$tmpdir/excluded" '
  BEGIN { while ((getline line < excluded) > 0) bad[line] = 1 }
  function clean(list,   n, i) {
    n = split(list, imports, " ")
    for (i = 1; i <= n; i++) if (imports[i] in bad) return 0
    return 1
  }
  {
    ip[NR] = $1
    notest[NR] = ($2 == "notest")
    deps[NR] = notest[NR] ? $3 : $2
    if ($1 ~ /\.test$/) closure[substr($1, 1, length($1) - 5)] = deps[NR]
  }
  END {
    for (i = 1; i <= NR; i++) {
      p = ip[i]
      if (p !~ /^github\.com\/ray-project\/ray\/go\//) continue
      if (p ~ /[ \[\]]/ || p ~ /\.test$/) continue
      if (notest[i] || p in bad || !clean(deps[i])) continue
      if ((p in closure) && !clean(closure[p])) continue
      print p
    }
  }' "$tmpdir/listed"
