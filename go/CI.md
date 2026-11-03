# Go runtime CI

The `go-runtime` workflow (`.github/workflows/go-runtime.yml`) is the repository's hosted-runner gate for the Go runtime. It runs on pull requests touching `go/**` (or the Python switch layer it launches through) and on pushes to master. This page states honestly what it covers and, just as important, what it does not.

## What CI runs

**Fast hooks.** The fast subset of the repository's pre-commit config, run by hook id: trailing-whitespace, check-added-large-files, check-ast, black, check-json, check-toml, end-of-file-fixer, ruff, prettier and buildifier.

**Pure-Go build, vet and test.** `go/scripts/list_pure_go_packages.sh` derives, mechanically, the set of packages that compile and test under a plain Go toolchain, and the job runs `go build`, `go vet` and `go test` on exactly the printed set — nothing is added by hand. A package is excluded when any of its non-test files use cgo, when it transitively depends on such a package, when any package in its dependency closure fails to load, or when it has no non-test files at all (a test-only package cannot be built by `go build` standalone). The load rule is what keeps the set honest: the generated protobuf sources under `go/proto` exist only as bazel build outputs and are absent from the source tree, so every package that transitively imports them cannot compile without bazel, and the script drops it before the job can. The script resolves the module graph against a throwaway copy of `go.mod`, so running it never modifies the working tree.

**`go.mod` self-consistency.** The four-module `go get` has landed (`github.com/coder/websocket`, `github.com/sabhiram/go-gitignore`, `golang.org/x/crypto`, `golang.org/x/sync`), so the repo-wide failure of a plain `go list ./...` on missing `require` lines is fixed: the module graph now resolves in readonly mode against the checked-in `go.mod`/`go.sum`, which is what lets `go build`, `go vet` and `go test` run without the script's temp-modfile fallback. A bare `go list ./...` still names the three bazel-only cgo packages (`go/e2e/driver_call_cpp`, `go/cmd/plugin`, `go/internal/runtime/native`), exactly as before this change — they are the packages the derivation above already excludes, and they need the C++ core build, not a `require` line. `go mod tidy` remains unavailable: acting as if all build tags were enabled, it must resolve the `//go:build cloud` file in `internal/runtime_env`, which pulls in `google.golang.org/api`, which needs go >= 1.26 — so targeted `go get` is the maintenance path for new requires.

**Fleet configuration dry-run.** `go/e2e/run_fleet.sh` is checked with `bash -n` and through its own driver-table validation: an unknown `--driver` must be rejected, and that check runs before the environment preflight, so it is exercisable without a cluster or the ray CLI. The script's full `--dry-run` plan walk is not runnable here: its preflight requires the `ray` CLI on PATH even in dry-run mode, and its Go-artifact checks expect a packaged runtime (`raygo`, `go_runtime.so`) that a hosted runner does not have.

**Python switch tests (advisory).** A `continue-on-error` job installs the latest ray wheel PyPI offers, pre-releases included (`pip install --pre ray` — this is the latest release channel, not a nightly build), overlays this tree's switch layer (`python/ray/_private/services.py` and `python/ray/_private/ray_constants.py`) on top of it, and runs the go/raygo-marked launch tests in `python/ray/tests/test_logging.py`. It is advisory because those tests exercise a compiled runtime that this checkout does not build; a red run signals a switch-layer drift but does not block the merge.

## What stays with the contributor

Everything that needs the C++ core libraries or a cluster, which hosted runners do not provide. Concretely: the cgo packages (they link the C++ core), the packages that import the bazel-generated protobuf sources, the bazel test union that covers both, the cross-language lanes (Go against Python and C++), and the real-cluster end-to-end fleet driven by `go/e2e/run_fleet.sh`. Before submitting a change, run the full Go test union locally:

```bash
bazel test //go/...
```

In the current tree, the packages the derivation script drops from the hosted-runner set are the ones that need the bazel-built C++ core libraries or the generated protobuf sources, which only the local union gate covers; the derivation can also drop a package for an undeclared module import, so treat the set as derived, not guaranteed.
