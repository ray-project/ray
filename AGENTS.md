# AGENTS.md — Eugo Ray-Meson Fork

> **Eugo — The Future of Supercomputing**
>
> This repository is a **public Meson-based fork** of [ray-project/ray](https://github.com/ray-project/ray) maintained by Eugo, a high-performance computing company.
> We replace Ray's Bazel build system with [Meson](https://mesonbuild.com/) and make targeted code modifications for HPC environments including GPU cluster scheduling (CUDA), InfiniBand/high-speed networking, and general HPC packaging and deployment.

---

## Table of Contents

1. [Repository Overview](#1-repository-overview)
2. [Build System Architecture](#2-build-system-architecture)
3. [Bazel-to-Meson Translation Guide](#3-bazel-to-meson-translation-guide)
4. [eugo_sync.py — Source File Tracker](#4-eugo_syncpy--source-file-tracker)
5. [EUGO Change Annotation Convention](#5-eugo-change-annotation-convention)
6. [Upstream Sync Process](#6-upstream-sync-process)
7. [Files and Ownership Rules](#7-files-and-ownership-rules)
8. [Common Sync Scenarios](#8-common-sync-scenarios)
9. [Build and Validation](#9-build-and-validation)
10. [Dependency Management](#10-dependency-management)
11. [Key Gotchas and Pitfalls](#11-key-gotchas-and-pitfalls)
12. [Directory Reference](#12-directory-reference)
13. [Rules for AI Coding Agents](#13-rules-for-ai-coding-agents)
14. [Step 2: Shared Library Consolidation](#14-step-2-shared-library-consolidation)

---

## 1. Repository Overview

| Property | Value |
|----------|-------|
| **Upstream** | `ray-project/ray` `master` branch (nightly) |
| **Fork purpose** | Replace Bazel with Meson; HPC packaging/deployment |
| **Ray version tracked** | `3.0.0.dev0` (master/nightly) |
| **Build backend** | `meson-python` (PEP 517) via `pyproject.toml` |
| **C++ standard** | `gnu++23` |
| **C standard** | `gnu17` |
| **Python requirement** | `>=3.12` |
| **Meson requirement** | `>=1.5.2` (build), `>=1.3.0` (project) |

### What This Fork Does

1. **Replaces Bazel entirely** — all `BUILD.bazel` / `WORKSPACE` / `bazel/` files from upstream are inert. The actual build is driven by `meson.build` files throughout the tree.
2. **Centralizes external C++ dependencies** — all external dependency declarations live in `eugo/dependencies/meson.build` using Meson's `dependency()` and `find_program()`.
3. **Manages code generation** — protobuf (C++/Python), gRPC, and FlatBuffers codegen is handled via reusable Meson `custom_target` kwargs defined in `eugo/utils/meson.build`.
4. **Vendors minimal headers** — e.g., GTest stubs in `eugo/include/gtest/` and OpenCensus protos in `eugo/include/opencensus/`.
5. **Makes targeted C++ fixes** — annotated with `@EUGO_CHANGE` markers for traceability during upstream syncs.
6. **Restructures Python packaging** — thirdparty packages that were vendored in Bazel-built Ray are declared as proper pip dependencies in `pyproject.toml`.

---

## 2. Build System Architecture

### Top-Level Build Graph

```
meson.build (root)
├── eugo/                         # EUGO build infrastructure (OURS — never overwrite)
│   ├── dependencies/meson.build  # All external C++ deps (absl, boost, grpc, protobuf, etc.)
│   ├── utils/meson.build         # Codegen helpers (protoc, flatc wrappers)
│   ├── utils/patching_protoc_py.sh  # Python proto import path fixer
│   └── include/                  # Vendored headers (gtest stubs, opencensus protos)
├── src/                          # C++ source → ~30 static libraries
│   ├── proto/                    # .proto and .fbs source files
│   └── ray/                     # Careful subdir ordering to handle circular deps
│       ├── protobuf/ → thirdparty/ → util/ → stats/ → flatbuffers/ → common/ → ray_syncer/ → rpc/
│       ├── Then: stats_cpp_lib (inline, breaks circular dep)
│       ├── Then: object_manager/ → pubsub/ → raylet_ipc_client/ → raylet_rpc_client/ → gcs/
│       ├── Then: gcs_rpc_client/ → raylet/scheduling/ → gcs/gcs_server/ → observability/
│       ├── Then: core_worker_rpc_client/ → core_worker/
│       ├── Then: object_manager_cpp_lib (inline, breaks circular dep)
│       └── Then: raylet/ → internal/
└── python/                       # Python package
    ├── ray/meson.build           # _version.py generation (git SHA injection)
    ├── meson.build               # _raylet Cython extension + pure Python install + dashboard
    └── ray/dashboard/client/     # npm ci + npm run build
```

### Meson Target Patterns

Every Bazel `ray_cc_library()` target maps to one of the Meson patterns below. The pattern depends on whether the target has source files (`.cc`), only headers (`.h`), or involves code generation. **The goal is a 1:1 mapping from Bazel targets to Meson targets** — see §3 for the two-stage translation approach.

#### Naming Convention

| Target Type | Bazel Name | Meson Variables |
|---|---|---|
| Static library | `status` | `status_cpp_lib_dependencies`, `status_cpp_lib`, `status_cpp_lib_dep` |
| Header-only library | `macros` | `macros_cpp_lib_dep` (+ optional `_dependencies`) |
| FlatBuffers codegen | `plasma_fbs` | `plasma_cpp_fbs` (custom_target), `plasma_cpp_fbs_lib_dep` |
| C++ Protobuf | `X_cc_proto` | `X_proto_cpp` (custom_target), `X_proto_cpp_dep`, `X_proto_cpp_lib`, `X_proto_cpp_lib_dep` |
| C++ gRPC | `X_cc_grpc` | `X_proto_cpp_grpc` (custom_target), `X_proto_cpp_grpc_dep`, `X_proto_cpp_grpc_lib`, `X_proto_cpp_grpc_lib_dep` |
| Python Protobuf | `X_py_proto` | `X_proto_py` (custom_target) |

#### Pattern 1: Static Library

For Bazel targets with `srcs` (`.cc` files). Uses three variables because **static libraries do not transitively export their dependencies to consumers** — the `declare_dependency()` wrapper bundles the library with all its dependencies so downstream targets inherit them automatically.

```meson
foo_cpp_lib_dependencies = [
    # Package-managed
    bar_cpp_lib_dep,
    baz_cpp_lib_dep,

    # Eugo-managed
    absl_time,
    protobuf,
    threads,
]

foo_cpp_lib = static_library('foo_cpp_lib',
    ['file1.cc', 'file2.cc'],
    install: false,
    dependencies: foo_cpp_lib_dependencies
)

foo_cpp_lib_dep = declare_dependency(
    link_with: [foo_cpp_lib],
    dependencies: foo_cpp_lib_dependencies
)
```

**Why the `_dep` wrapper?** A `static_library()` on its own does not re-export its dependencies. Without the wrapper, consumers get undefined symbols at link time. The `declare_dependency()` ensures that any target listing `foo_cpp_lib_dep` automatically inherits both the library linkage (`link_with`) and all transitive dependencies. This avoids both undefined symbols (from missing deps) and duplicate symbols (from merging object files across multiple static libraries in the final `_raylet` shared library).

**Example:** `status_cpp_lib` in `src/ray/common/meson.build`

#### Pattern 2: Header-Only Library

For Bazel targets with only `hdrs` and no `srcs`. **Do NOT create a `static_library()`** — header-only targets have no object code to compile, and doing so causes duplicate symbol errors or compiles code unnecessarily.

Every header-only target MUST:
1. Have a `# Header-only` comment immediately after the `@begin` marker.
2. Extract dependencies into a separate `_dependencies` variable (never inline them in the `declare_dependency()` call).
3. Use section comments (`# Package-managed`, `# Eugo-managed`) in the `_dependencies` list, same as Pattern 1.

```meson
# === @begin: noop_cgroup_manager_cpp_lib (@original: noop_cgroup_manager) ===
# Header-only
noop_cgroup_manager_cpp_lib_dependencies = [
    # Package-managed
    cgroup_driver_interface_cpp_lib_dep,
    cgroup_manager_interface_cpp_lib_dep,
    status_cpp_lib_dep,
    status_or_cpp_lib_dep,
]

noop_cgroup_manager_cpp_lib_dep = declare_dependency(
    sources: ['noop_cgroup_manager.h'],
    dependencies: noop_cgroup_manager_cpp_lib_dependencies
)
# === @end: noop_cgroup_manager_cpp_lib ===
```

If a header-only target has zero dependencies, the `_dependencies` variable and section comments may be omitted:

```meson
# === @begin: constants_cpp_lib (@original: constants) ===
# Header-only
constants_cpp_lib_dep = declare_dependency(
    sources: ['constants.h']
)
# === @end: constants_cpp_lib ===
```

**Example:** `noop_cgroup_manager_cpp_lib_dep` in `src/ray/common/cgroup2/meson.build`

#### Pattern 3: FlatBuffers Codegen (Header-Only)

For Bazel `flatbuffer_cc_library()` targets (suffix `_fbs`). Uses `custom_target` with `cpp_fbs_default_kwargs` from `eugo/utils/meson.build`. Always include `flatbuffers` in the dependency so downstream targets can resolve FlatBuffers symbols.

```meson
# === @begin: plasma_cpp_fbs_lib (@original: plasma_fbs) ===
plasma_cpp_fbs = custom_target(
    input: ['plasma.fbs'],
    kwargs: cpp_fbs_default_kwargs
)

# header-only
plasma_cpp_fbs_lib_dep = declare_dependency(
    sources: [plasma_cpp_fbs],
    dependencies: [flatbuffers]
)
# === @end: plasma_cpp_fbs_lib (@original: plasma_fbs) ===
```

**Example:** `plasma_cpp_fbs_lib_dep` in `src/ray/flatbuffers/meson.build`

#### Pattern 4: C++ Protobuf

For Bazel `cc_proto_library()` targets. Produces: `custom_target` (codegen) → virtual `declare_dependency` (with proto `import` deps as its dependencies) → `static_library` → consumer `declare_dependency`. The virtual wrapper is needed because the generated header files must be visible to downstream targets.

```meson
# @begin: proto_cpp (@original: node_manager_cc_proto)
node_manager_proto_cpp = custom_target(
    input: node_manager_proto,
    kwargs: proto_cpp_default_kwargs
)

node_manager_proto_cpp_dep = declare_dependency(
    sources: node_manager_proto_cpp,
    dependencies: [common_proto_cpp_dep, gcs_proto_cpp_dep, autoscaler_proto_cpp_dep]
)

node_manager_proto_cpp_lib_dependencies = [
    node_manager_proto_cpp_dep,
    protobuf
]

node_manager_proto_cpp_lib = static_library(
    'node_manager_proto_cpp_lib',
    dependencies: node_manager_proto_cpp_lib_dependencies,
    install: false
)

node_manager_proto_cpp_lib_dep = declare_dependency(
    link_with: [node_manager_proto_cpp_lib],
    dependencies: node_manager_proto_cpp_lib_dependencies
)
# @end: proto_cpp (@original: node_manager_cc_proto)
```

**Example:** `node_manager_proto_cpp_lib_dep` in `src/ray/protobuf/meson.build`

#### Pattern 5: C++ gRPC Protobuf

For Bazel `cc_grpc_library()` targets. Always requires the corresponding C++ Protobuf target (Pattern 4) as a dependency:

```meson
# @begin: proto_cpp_grpc (@original: node_manager_cc_grpc)
node_manager_proto_cpp_grpc = custom_target(
    input: node_manager_proto,
    kwargs: proto_cpp_grpc_default_kwargs
)

node_manager_proto_cpp_grpc_dep = declare_dependency(
    sources: node_manager_proto_cpp_grpc
)

node_manager_proto_cpp_grpc_lib_dependencies = [
    node_manager_proto_cpp_grpc_dep,
    node_manager_proto_cpp_lib_dep,  # Always depends on Pattern 4
    protobuf,
    grpc
]

node_manager_proto_cpp_grpc_lib = static_library(
    'node_manager_proto_cpp_grpc_lib',
    dependencies: node_manager_proto_cpp_grpc_lib_dependencies,
    install: false
)

node_manager_proto_cpp_grpc_lib_dep = declare_dependency(
    link_with: [node_manager_proto_cpp_grpc_lib],
    dependencies: node_manager_proto_cpp_grpc_lib_dependencies
)
# @end: proto_cpp_grpc (@original: node_manager_cc_grpc)
```

#### Pattern 6: Python Protobuf + gRPC

Always produces both Protobuf and gRPC Python files. No dependency on C++ Protobuf targets. Installation path is handled by `proto_py_default_kwargs`.

```meson
# @begin: proto_py (@original: node_manager_py_proto)
node_manager_proto_py = custom_target(
    input: node_manager_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (@original: node_manager_py_proto)
```

> **Note:** Not all `.proto` files need all target types. Check the upstream `BUILD.bazel` to determine which targets to produce for each `.proto`.

### Build Scripts

| Script | Purpose |
|--------|---------|
| `eugo_bootstrap.sh` | One-time environment setup (Node.js, directories) |
| `eugo_meson_setup.sh` | `meson setup --reconfigure eugo_build` with warning suppression flags |
| `eugo_meson_compile.sh` | `meson compile -C eugo_build` |
| `eugo_pip3_wheel.sh` | `pip3 wheel .` using `meson-python` backend (build dir: `eugo_build_whl`) |
| `eugo_sync.py` | Verification: finds C/C++ source files in `src/` not referenced by any `meson.build` |

---

## 3. Bazel-to-Meson Translation Guide

The upstream Bazel `BUILD.bazel` files are the **source of truth** for which `.cc` files belong to which logical libraries. Understanding how Bazel targets map to Meson `static_library()` calls is critical for validating correctness during syncs.

### Two-Stage Translation Approach

When translating Bazel to Meson during upstream syncs, follow a strict two-stage process:

**Stage 1: Replicate Bazel 1:1** — Create one Meson target for every `ray_cc_library()` in `BUILD.bazel`, using the appropriate pattern from §2. This ensures correctness and makes debugging straightforward — if a linker error occurs, you know exactly which target is misconfigured.

**Stage 2: Consolidate (optional, separate commit)** — After Stage 1 builds clean, adjacent targets within the same directory *may* be merged into fewer Meson libraries. This is purely an optimization and requires understanding the dependency graph.

> **Critical:** Do NOT skip Stage 1. Consolidating during initial translation makes it much harder to debug linker errors, verify correctness, and identify missing dependencies or source files. Every skipped Bazel target is a potential source of hard-to-trace build failures.

### Target Ordering Rule

**The order of `# === @begin` blocks in a `meson.build` MUST match the order of `ray_cc_library()` targets in the corresponding `BUILD.bazel`**, with one exception: when a target depends on another target defined in the *same* `meson.build` file, Meson's define-before-use semantics require the dependency to appear first — only deviate from Bazel order in that case.

This 1:1 ordering makes manual cross-referencing fast and ensures no targets are accidentally skipped during the initial translation and subsequent consolidation into a shared library.

**Example:** Given this `BUILD.bazel`:

```bazel
ray_cc_library(
    name = "noop_cgroup_manager",
    hdrs = ["noop_cgroup_manager.h"],
    deps = [
        ":cgroup_driver_interface",
        ":cgroup_manager_interface",
        "//src/ray/common:status",
        "//src/ray/common:status_or",
    ],
)

ray_cc_library(
    name = "cgroup_driver_interface",
    hdrs = ["cgroup_driver_interface.h"],
    deps = [
        "//src/ray/common:status",
        "//src/ray/common:status_or",
    ],
)
```

The `meson.build` should preserve the same top-to-bottom ordering:

```meson
# === @begin: noop_cgroup_manager_cpp_lib (@original: noop_cgroup_manager) ===
noop_cgroup_manager_cpp_lib_dependencies = [
    cgroup_driver_interface_cpp_lib_dep,
    cgroup_manager_interface_cpp_lib_dep,
    status_cpp_lib_dep,
    status_or_cpp_lib_dep,
]

noop_cgroup_manager_cpp_lib_dep = declare_dependency(
    sources: ['noop_cgroup_manager.h'],
    dependencies: noop_cgroup_manager_cpp_lib_dependencies
)
# === @end: noop_cgroup_manager_cpp_lib ===


# === @begin: cgroup_driver_interface_cpp_lib (@original: cgroup_driver_interface) ===
cgroup_driver_interface_cpp_lib_dependencies = [
    status_cpp_lib_dep,
    status_or_cpp_lib_dep
]

cgroup_driver_interface_cpp_lib_dep = declare_dependency(
    sources: ['cgroup_driver_interface.h'],
    dependencies: cgroup_driver_interface_cpp_lib_dependencies
)
# === @end: cgroup_driver_interface_cpp_lib ===
```

This works because `cgroup_driver_interface_cpp_lib_dep` is defined elsewhere (processed earlier via `subdir()`). If it were defined only in this same file, it would have to move before `noop_cgroup_manager` — that is the one allowable ordering deviation, and it must be documented with a comment explaining why Bazel order cannot be followed.

**When ordering deviates from BUILD.bazel**, the `meson.build` file MUST have an `# IMPORTANT` comment block at the very top of the file (before any target definitions) explaining:
1. That the ordering within this file respects the internal Meson dependency graph, NOT the BUILD.bazel order.
2. Why deviations exist (e.g., define-before-use constraints, targets inlined from subdirectories, circular dependency resolution).

Example header:
```meson
# IMPORTANT: Ordering within this file respects the internal dependency graph.
# You will see things "out of order" from a BUILD.bazel perspective because
# Meson requires define-before-use: if target A depends on target B and both
# are in this file, B must appear first regardless of Bazel ordering.
```

This ensures anyone cross-referencing against BUILD.bazel immediately understands the ordering rationale instead of assuming targets were accidentally misordered.

### Translating Each Bazel Target

For each `ray_cc_library()` in a `BUILD.bazel`:

1. **Check `srcs` vs `hdrs`**: Has `.cc` files → Pattern 1 (static library). Only `.h` files → Pattern 2 (header-only).
2. **Map ALL `deps`** — every Bazel dependency must have a Meson equivalent:
   - `:local_target` → the corresponding `*_cpp_lib_dep` in the same `meson.build`
   - `//src/ray/foo:bar` → `bar_cpp_lib_dep` from `src/ray/foo/meson.build`
   - `@external//...` → variable from `eugo/dependencies/meson.build` (see §10 mapping table)
3. **Handle `select()`**: Bazel platform conditionals become `if host_machine.system() == 'linux'` in Meson.
4. **Handle visibility**: Bazel visibility has no Meson equivalent — all Meson targets are accessible within the project. Ignore visibility settings.

### Validation Procedure

To verify a `meson.build` matches its Bazel counterpart, follow ALL steps:

#### Step 1: Target Count Audit (MANDATORY)

Before writing any Meson code, count the production (non-test) `ray_cc_library()` targets in the `BUILD.bazel`. The `meson.build` MUST define exactly one Meson target per Bazel target. If the counts don't match, the translation is incomplete.

```bash
# Count production Bazel targets (exclude test targets)
grep 'name = "' src/ray/<component>/BUILD.bazel | grep -v test

# Count Meson targets (each @begin/@end block = 1 target)
grep '@begin:' src/ray/<component>/meson.build

# These counts MUST match (modulo intentional exclusions documented in the file header)
```

#### Step 2: Target-by-Target Checklist (MANDATORY)

For EACH Bazel target, verify:

1. **Existence**: A corresponding Meson target exists with the correct naming (`<name>_cpp_lib_dep`).
2. **Source files**: Every `.cc` file in Bazel `srcs` appears in the Meson `static_library()`. Every `.h` file in Bazel `hdrs` is listed in `declare_dependency(sources: ...)` for header-only targets.
3. **Header-only check**: If Bazel target has no `srcs` → Meson uses Pattern 2 (NO `static_library()`). If Bazel target has `srcs` → Meson uses Pattern 1 (static library).
4. **Dependencies**: Every entry in the Bazel `deps` list has a corresponding Meson dep. Use the mapping table in §10 for external deps.
5. **No consolidation**: Multiple Bazel targets are NOT merged into one Meson target. Each must be separate.

#### Step 3: Dependency Existence Check

For each dependency referenced in the Meson file, verify it actually exists (is defined in another `meson.build` that is processed BEFORE this one):

```bash
# For each dep like foo_cpp_lib_dep used in the file:
grep -rn 'foo_cpp_lib_dep =' src/ray/**/meson.build
# Must find exactly one definition, and it must be in a meson.build processed earlier
```

#### Step 4: Downstream Consumer Check

Verify that any existing consumers of old consolidated targets still work:

```bash
# If you're breaking consolidated_cpp_lib_dep into individual targets:
grep -rn 'consolidated_cpp_lib_dep' src/ray/**/meson.build
# Each consumer must be updated to use the correct individual target, OR
# an umbrella alias must be provided for backward compatibility
```

#### Step 5: Source File Sync

```bash
python3 eugo_sync.py
# Must report only expected test/thirdparty files
```

### Files That Are Intentionally Excluded From Meson

Some files in `src/ray/` appear in Bazel builds but are **intentionally not** in Meson:

| Category | Examples | Reason |
|----------|----------|--------|
| **Test files** | `*_test.cc`, `test_utils.cc`, `*_test_fixture.cc` | Not building tests in Meson |
| **Test utilities** | `cgroup_test_utils.cc`, `memory_monitor_test_fixture.cc` | Test support code |
| **Vendored thirdparty** | `src/ray/thirdparty/setproctitle/*.c` | We use pip `setproctitle` package instead |
| **Java JNI** | `src/ray/jni/**` | Not building Java bindings |

---

## 4. eugo_sync.py — Source File Tracker

### Purpose

`eugo_sync.py` is the primary tool for detecting C/C++ source files that exist on disk but are not referenced in any `meson.build` file. Run it after every upstream sync to catch newly added source files.

### How It Works

```
1. Scan src/ recursively for all .cc and .c files
2. Scan the entire repo for all meson.build files
3. Filter out files matching exclude patterns (test files, java/)
4. For each remaining source file, check if its FILENAME appears
   in ANY meson.build file (substring match)
5. Report files whose filename doesn't appear anywhere
```

### Key Implementation Details

- **Matching is by filename only** (e.g., `id.cc`), not by full path. The function `does_file_contains_string()` searches for the filename as a substring in each `meson.build` file's content.
- **Exclude patterns** filter out:
  - Files in directories named `java/` or `test/` (case-insensitive)
  - Files ending in `test.cc`
  - Files ending in `test_util.cc`
- **Exit status:** Reports "No raptors found! Great job." when all non-excluded source files are tracked.

### Limitations and Gotchas

| Limitation | Impact | Mitigation |
|-----------|--------|------------|
| **Filename-only matching** can produce false negatives | If two files with the same name exist in different directories (e.g., `common.cc` in both `object_manager/` and `rpc/`), one could mask the other | Manually verify files with common names appear in the *correct* `meson.build` |
| **Comment references count as "tracked"** | A commented-out `# 'id.cc'` still satisfies the match | Review delete comments carefully — the file may appear tracked but not actually compiled |
| **No path validation** | A file could appear in the *wrong* `meson.build` and still pass | Cross-reference against `BUILD.bazel` for correctness |
| **Test file exclusion is heuristic** | Files like `test_utils.cc` or `memory_monitor_test_fixture.cc` that don't match the patterns slip through | These show up as false-positive "untracked" — they're expected |

### Expected "Untracked" Files

After a clean sync, `eugo_sync.py` may report these files that are **intentionally not in any `meson.build`**:

- `src/ray/thirdparty/setproctitle/*.c` — replaced by pip package
- `src/ray/common/test_utils.cc` — test utility
- `src/ray/common/memory_monitor_test_fixture.cc` — test fixture
- `src/ray/common/cgroup2/cgroup_test_utils.cc` — test utility
- `src/ray/rpc/authentication/tests/grpc_auth_token_tests.cc` — test file

### Usage

```bash
# Standard run
python3 eugo_sync.py

# Expected clean output:
# "No raptors found! Great job."

# If files are reported, for each one:
# 1. Check if it's a test file → ignore
# 2. Check if it's intentionally excluded (thirdparty vendor) → ignore
# 3. Otherwise → find the right BUILD.bazel target → add to correct meson.build
```

---

## 5. EUGO Change Annotation Convention

All code modifications to upstream Ray files **must** be annotated with `@EUGO_CHANGE` markers. This is critical for traceability during upstream syncs.

### Annotation Formats

#### Active Change (block)

```cpp
// @EUGO_CHANGE: @begin: <reason for the change>
<modified code>
// @EUGO_CHANGE: @end
```

Optionally, the original code can be kept as a comment inside the block for reference:

```cpp
// @EUGO_CHANGE: @begin: to fix the issue with non-UTF8 filenames
// std::string filename{file.path().filename().u8string()};
std::string filename{file.path().filename().string()};
// @EUGO_CHANGE: @end
```

#### Active Change (inline, for single lines or config annotations)

```python
# @EUGO_CHANGE:
# These and `[project.optional-dependencies]` should be synced against `setup.py`...
```

#### Historical / No-Longer-Needed Change (for posterity)

When an EUGO change was needed with older library versions but the upstream code has since caught up or the issue no longer applies, keep it documented with `@NO_CHANGE`:

```cpp
// @EUGO_CHANGE: @begin: @NO_CHANGE: For posterity, we needed the following as we supported newer version of protobuf
// reply_ = google::protobuf::Arena::Create<Reply>(&arena_);
reply_ = google::protobuf::Arena::CreateMessage<Reply>(&arena_);
// @EUGO_CHANGE: @end: @NO_CHANGE
```

### Summary Table

| Format | Meaning |
|--------|---------|
| `@EUGO_CHANGE: @begin: <reason>` ... `@EUGO_CHANGE: @end` | Active code modification with explanation |
| `@EUGO_CHANGE: @begin: @NO_CHANGE: <reason>` ... `@EUGO_CHANGE: @end: @NO_CHANGE` | Historical — kept for posterity, change no longer active |
| `@EUGO_CHANGE:` (standalone) | Configuration note or inline annotation |

### Rules for Annotations

1. **Always annotate** any modification to an upstream file — no exceptions.
2. **Always include a reason** — explain *why* the change was made, not just *what*.
3. **Keep the original code** as a comment inside the block when practical.
4. **Promote to `@NO_CHANGE`** when a previously-needed fix is resolved upstream — do not delete the annotation; it serves as documentation for future syncs.
5. **Language-appropriate comment syntax**: Use `//` for C/C++, `#` for Python/shell/meson, `<!-- -->` for XML/HTML.

---

## 6. Upstream Sync Process

We track `ray-project/ray` `master` (nightly). Syncing is a combination of automated scripts and manual cherry-pick/merge.

### Sync Workflow

```
1. Fetch upstream master
2. Identify changed files (automated diff)
3. Classify files by ownership (see §7)
4. Apply upstream changes to UPSTREAM-owned files
5. For SHARED files, manually merge — preserve @EUGO_CHANGE blocks
6. Run eugo_sync.py to detect new untracked source files
7. Update meson.build files for any new/removed source files
8. Build and validate (see §9)
9. Compare output wheel against upstream wheel
```

### Post-Sync Checklist

- [ ] `eugo_sync.py` reports "No raptors found!" (all source files tracked)
- [ ] `eugo_meson_setup.sh` succeeds
- [ ] `eugo_meson_compile.sh` succeeds
- [ ] `eugo_pip3_wheel.sh` builds a wheel
- [ ] Wheel contents match upstream wheel (compare via diffchecker/Beyond Compare)
- [ ] All `@EUGO_CHANGE` blocks are intact and still valid
- [ ] No new Bazel-only dependencies that need Meson equivalents
- [ ] `pyproject.toml` dependencies are in sync with `setup.py` and `requirements.txt`

---

## 7. Files and Ownership Rules

### EUGO-OWNED (never overwrite from upstream)

These files exist only in our fork or have been entirely replaced. **Never accept upstream changes to these.**

| Path | Description |
|------|-------------|
| `eugo/**` | All Eugo build infrastructure |
| `meson.build` (root) | Top-level Meson build definition |
| `python/meson.build` | Python package build: _raylet, install, dashboard |
| `python/ray/meson.build` | Version file generation |
| `src/meson.build` | C++ src entry point |
| `src/ray/meson.build` | C++ library ordering and circular dep resolution |
| `src/ray/*/meson.build` | All ~30 C++ component build files |
| `src/proto/*/meson.build` | Proto/FlatBuffers codegen directives |
| `python/ray/dashboard/client/meson.build` | Dashboard JS build |
| `pyproject.toml` | Build backend, deps (meson-python, not setuptools) |
| `eugo_*.sh` / `eugo_*.py` | All Eugo build/sync scripts |
| `AGENTS.md` | This file |

### UPSTREAM-OWNED (accept upstream changes freely)

These files come directly from upstream and should be overwritten during sync without reservation.

| Path | Description |
|------|-------------|
| `src/ray/**/*.cc` (without `@EUGO_CHANGE`) | C++ source files |
| `src/ray/**/*.h` (without `@EUGO_CHANGE`) | C++ headers |
| `src/ray/protobuf/**/*.proto` | Protobuf definitions |
| `src/ray/raylet/format/**/*.fbs` | FlatBuffers schemas |
| `python/ray/**/*.py` (without `@EUGO_CHANGE`) | Python source code |
| `python/ray/**/*.pyx` / `*.pxd` | Cython source files |
| `doc/**` | Documentation |
| `java/**` | Java code (not built by Meson, kept for completeness) |
| `rllib/**` | RLlib code |
| `release/**` | Release tooling |

### SHARED (merge carefully — preserve EUGO annotations)

These files exist upstream but contain `@EUGO_CHANGE` blocks that must be preserved.

| Path | Description |
|------|-------------|
| `src/ray/common/memory_monitor.cc` | Non-UTF8 filename fix |
| `src/ray/rpc/server_call.h` | Protobuf API version fix |
| `src/ray/util/event.cc` | Protobuf version conditional |
| `src/ray/object_manager/common.cc` | absl include fix |
| `src/ray/object_manager/plasma/protocol.cc` | FlatBuffers API fix |
| Any file with `@EUGO_CHANGE` markers | Must be merged manually |

> **How to find all SHARED files:**
> ```bash
> grep -rl "@EUGO_CHANGE" --include="*.cc" --include="*.h" --include="*.py" --include="*.toml" .
> ```

### INERT (kept but not used by Meson build)

| Path | Description |
|------|-------------|
| `BUILD.bazel` / `WORKSPACE` / `bazel/**` | Upstream Bazel build system — inert in our fork |
| `build.sh` | Upstream Bazel build entry point |
| `setup.py` | Upstream setuptools config — we use `pyproject.toml` + `meson-python` |
| `python/build-wheel-*.sh` | Upstream wheel build scripts |

---

## 8. Common Sync Scenarios

### 8.1. New C++ Source Files Added Upstream

**Symptom:** `eugo_sync.py` reports files that aren't tracked in any `meson.build`.

**Resolution:**
1. Identify which library the file belongs to (check the upstream `BUILD.bazel` for the target — see §3 for the Bazel-to-Meson translation guide).
2. Add the file to the appropriate `static_library()` call in `src/ray/<component>/meson.build`.
3. If it's a new component entirely, create a new `meson.build` following the standard library pattern (§2) and add a `subdir()` call in the parent `meson.build` at the correct position in the dependency ordering.

### 8.2. C++ Source Files Removed Upstream

**Symptom:** Meson build fails with "file not found".

**Resolution:**
1. Remove the file reference from the relevant `meson.build`.
2. Remove the file itself.

### 8.3. New External Dependency Added Upstream

**Symptom:** Meson build fails with undefined symbols or missing headers.

**Resolution:**
1. Add the dependency in `eugo/dependencies/meson.build` using `dependency()` with the appropriate method (`cmake`, `pkg-config`, etc.).
2. Add the dependency variable to the relevant library's `_dependencies` list.

### 8.4. Protobuf Schema Changes

**Symptom:** Build fails during codegen or downstream compilation.

**Resolution:**
1. If new `.proto` files: add a `custom_target()` in `src/ray/protobuf/meson.build` (for C++) and the Python proto `meson.build` using the kwargs from `eugo/utils/meson.build`.
2. If modified `.proto` files: usually just rebuilds automatically.
3. If deleted `.proto` files: remove the corresponding `custom_target()` entries.
4. Check `eugo/utils/patching_protoc_py.sh` if Python import paths changed — see §8.4.1 below.

#### 8.4.1. Python Proto Import Patching (`patching_protoc_py.sh`)

All generated Python protobuf files (`*_pb2.py`, `*_pb2_grpc.py`) are installed into a single flat directory (`ray/core/generated/`). However, `protoc` generates import statements based on the `.proto` file's directory structure (e.g., `from src.ray.protobuf.public import foo_pb2`). These imports break at runtime because the files are flattened. The script `eugo/utils/patching_protoc_py.sh` fixes these imports via `sed` replacements.

**When new subdirectories are added** under `src/ray/protobuf/` (or any proto source path), the generated Python files will contain imports like `from ..subdirname import ...` or `from src.ray.protobuf.subdirname import ...`. A corresponding `sed` patch **must** be added to `patching_protoc_py.sh` to rewrite these to relative imports (`from . import ...`).

**Current patches (as of this writing):**

```bash
# MARK: - 1. Eugo-Vendored (opencensus protos from eugo/include/)
sed -i -E 's/from opencensus.proto.metrics.v1 import/from . import/' "${output}"
sed -i -E 's/from opencensus.proto.resource.v1 import/from . import/' "${output}"

# MARK: - 2. Ray-Provided
# MARK: - 2.1. Primary (top-level src/ray/protobuf/ protos)
sed -i -E 's/from src.ray.protobuf/from ./' "${output}"

# MARK: - 2.2. Secondary (subdirectory protos — add new entries here)
sed -i -E 's/from ..public/from ./' "${output}"
```

**History:** Previously, `src/ray/protobuf/` had `experimental/` and `export_api/` subdirectories, which required patches:
```bash
# REMOVED — these subdirectories no longer exist:
# sed -i -E 's/from ..experimental/from ./' "${output}"
# sed -i -E 's/from ..export_api/from ./' "${output}"
```
These were replaced by the `public/` subdirectory, so the current patch is `from ..public` → `from .`.

**Concrete example of the mapping:**
- `src/ray/protobuf/public/` exists → `protoc` generates `from ..public import foo_pb2` → we patch to `from . import foo_pb2`

**How to add a new patch:**
1. Identify the new subdirectory (e.g., `src/ray/protobuf/newdir/`).
2. Build the wheel or run the proto codegen.
3. Inspect any generated `*_pb2.py` file that imports from the new subdirectory — note the exact import path `protoc` generates (e.g., `from ..newdir import foo_pb2`).
4. Add a `sed` command to `patching_protoc_py.sh` under `# MARK: - 2.2. Secondary`:
   ```bash
   sed -i -E 's/from ..newdir/from ./' "${output}"
   ```
5. **Verify:** All `ray/core/generated/*.py` files should only import from the same directory (`.`) or from external packages (`google.protobuf`), never from relative parent paths (`..`) or absolute proto paths (`src.ray.protobuf`).

### 8.5. FlatBuffers Schema Changes

**Symptom:** Missing `*_generated.h` files.

**Resolution:**
Add/remove `custom_target()` entries in `src/ray/raylet/format/meson.build` using `cpp_fbs_default_kwargs`.

### 8.6. New Python Dependencies

**Symptom:** Import errors at runtime or missing packages.

**Resolution:**
1. Check upstream `setup.py` and `requirements.txt` for new entries.
2. Add them to `pyproject.toml` — under `[project.dependencies]` for core deps or `[project.optional-dependencies]` for optional groups.
3. The `@EUGO_CHANGE` annotation in `pyproject.toml` reminds you to keep these in sync.

### 8.7. Cython Changes (`_raylet.pyx`)

**Symptom:** Compilation errors in the `_raylet` extension module.

**Resolution:**
1. Accept the upstream `.pyx` / `.pxd` changes.
2. If new C++ libraries are referenced from Cython, add them to the `dependencies:` list in `python/meson.build`'s `py.extension_module('_raylet', ...)` call.

### 8.8. Upstream Modifies a File Containing `@EUGO_CHANGE`

**Resolution:**
1. **Do not blindly overwrite.** This is a SHARED file.
2. Manually merge — apply the upstream changes around the `@EUGO_CHANGE` blocks.
3. Verify the `@EUGO_CHANGE` is still needed:
   - If the upstream change fixes the same issue → promote to `@NO_CHANGE` and use upstream code.
   - If the upstream change conflicts → adapt the EUGO change and update the annotation reason.
   - If the upstream change is orthogonal → merge normally, keep annotation intact.

### 8.9. Dashboard (JavaScript) Changes

**Symptom:** Dashboard build fails or shows stale UI.

**Resolution:**
The Meson build runs `npm ci && npm run build` in `python/ray/dashboard/client/`. Upstream changes to `package.json` / `package-lock.json` should be accepted. The `meson.build` in that directory should not need changes unless the build command or output directory changes.

### 8.10. Empty Directory with Only a `meson.build` File

**Symptom:** A directory under `src/ray/` contains only a `meson.build` file and no source files (`.cc`, `.h`, `.proto`, etc.).

**Assessment procedure:**

1. **Check if source files still exist.** Do the source files referenced in the `meson.build` (in `static_library()` calls or `declare_dependency(sources: ...)`) still exist on disk? Search the entire tree — they may have been moved to a different directory during an upstream restructure.
2. **Check for duplicate dep definitions.** For every `*_dep` variable defined in this `meson.build`, search all other `meson.build` files to see if the same variable is already defined elsewhere in the active build graph.
3. **Check if the file is in the build graph.** Is this `meson.build` referenced by any `subdir()` call in a parent? If no `subdir()` points to it, it's unreachable dead code.

**Resolution decision tree:**

```
Directory has only meson.build, no source files?
├── All dep variables already defined elsewhere AND not subdir()'d?
│   └── DELETE the meson.build and the empty directory. Pure dead code.
├── Some dep variables NOT defined elsewhere?
│   ├── Can targets move to the logical parent meson.build (same subdir() position)?
│   │   └── MOVE targets to parent, adjust source paths, delete empty dir.
│   ├── Targets depend on things defined AFTER the parent in build ordering?
│   │   └── INLINE targets into src/ray/meson.build at the correct position
│   │       (like stats_cpp_lib and object_manager_cpp_lib). Delete empty dir.
│   └── Ordering allows it in parent?
│       └── MOVE to parent meson.build. Delete empty dir.
└── File is subdir()'d but references sources via ../ paths?
    └── Same as above — either INLINE into src/ray/meson.build at the
        correct ordering position, or MOVE to parent. Delete empty dir.
```

**Key principle:** Empty directories with only a `meson.build` should not exist. Either the targets are dead code (delete) or they belong in another file (move/inline).

---

## 9. Build and Validation

### Development Build

```bash
# One-time setup
./eugo_bootstrap.sh

# Configure
./eugo_meson_setup.sh

# Compile
./eugo_meson_compile.sh
```

### Wheel Build

```bash
./eugo_pip3_wheel.sh
```

This uses `meson-python` as the PEP 517 backend with build directory `eugo_build_whl`.

### Sync Verification

```bash
# Check for untracked C/C++ source files
python3 eugo_sync.py
# Expected output: "No raptors found! Great job."
```

### Wheel Comparison

After building, compare the EUGO wheel contents against the upstream Ray wheel:

1. Unpack both wheels.
2. Use a diff tool (Beyond Compare, diffchecker, etc.) to compare directory structures and file contents.
3. Expected differences: build metadata, `_version.py`, compiled extension paths.
4. Unexpected differences indicate missing files or incorrect `install_subdir()` exclusions in `python/meson.build`.

### Expected Build Artifacts

The Meson build must produce exactly these compiled outputs. Use this as the acceptance criterion when validating a build.

#### Executables

| Artifact | Install path in wheel | Description |
|---|---|---|
| `gcs_server` | `ray/core/src/ray/gcs/gcs_server` | GCS (Global Control Store) server process |
| `raylet` | `ray/core/src/ray/raylet/raylet` | Per-node scheduler/worker manager process |

#### Shared Libraries / Extensions

| Artifact | Install path in wheel | Description |
|---|---|---|
| `_raylet.so` | `ray/_raylet.so` | Cython extension — the main Python↔C++ bridge |

#### Intentionally Excluded from Our Wheel

The following artifacts appear in the upstream official wheel (`ray-*.whl`) but are **intentionally absent** from the Eugo wheel:

| Artifact | Reason | How we handle it |
|---|---|---|
| `ray/core/libjemalloc.so` | We do not vendor jemalloc | Link against system jemalloc or omit |
| `ray/thirdparty_files/psutil/_psutil_linux.abi3.so` | Installed globally as a pip package | Listed in `pyproject.toml` dependencies |
| `ray/_private/runtime_env/agent/thirdparty_files/aiohttp/*.so` | Installed globally as a pip package | Listed in `pyproject.toml` dependencies |
| `ray/_private/runtime_env/agent/thirdparty_files/propcache/*.so` | Installed globally as a pip package | Listed in `pyproject.toml` dependencies |
| `ray/_private/runtime_env/agent/thirdparty_files/frozenlist/*.so` | Installed globally as a pip package | Listed in `pyproject.toml` dependencies |
| `ray/_private/runtime_env/agent/thirdparty_files/multidict/*.so` | Installed globally as a pip package | Listed in `pyproject.toml` dependencies |
| `ray/_private/runtime_env/agent/thirdparty_files/yarl/*.so` | Installed globally as a pip package | Listed in `pyproject.toml` dependencies |

> **Verification command** (run inside the unpacked wheel directory):
> ```bash
> find . -type f -executable -exec file {} \; | grep -E 'ELF|Mach-O'
> # Expected output (Linux aarch64 example):
> # ./ray/core/src/ray/gcs/gcs_server: ELF 64-bit LSB pie executable ...
> # ./ray/core/src/ray/raylet/raylet: ELF 64-bit LSB pie executable ...
> # ./ray/_raylet.so: ELF 64-bit LSB shared object ...
> ```

---

## 10. Dependency Management

### External C++ Dependencies

**All** external C++ dependencies are declared in `eugo/dependencies/meson.build`. This is the single source of truth.

| Category | Dependencies |
|----------|-------------|
| **absl** | ~17 individual modules (core_headers, time, flat_hash_map, synchronization, etc.) |
| **boost** | fiber, thread (pulls in full Boost due to Meson limitation) |
| **protobuf** | libprotobuf + protoc (must use CMake method) |
| **gRPC** | gpr, grpc, grpc++, grpc++_reflection, channelz, opencensus_plugin |
| **flatbuffers** | library + flatc compiler |
| **spdlog** | via CMake |
| **msgpack-cxx** | via CMake |
| **nlohmann_json** | via CMake |
| **opencensus-cpp** | prometheus_exporter, stdout_exporter, stats, tags |
| **prometheus-cpp** | pull module |
| **hiredis** | hiredis + hiredis_ssl |
| **gflags** | via CMake |
| **threads** | system |
| **npm / git** | found via `find_program()` |

### Bazel → Meson Dependency Mapping

Complete mapping of Bazel external dependency labels to Meson variable names. All variables are defined in `eugo/dependencies/meson.build`.

#### Abseil (absl)

| Bazel Label | Meson Variable |
|---|---|
| `@com_google_absl//absl/base` | `absl_base` |
| `@com_google_absl//absl/base:core_headers` | `absl_base_core_headers` |
| `@com_google_absl//absl/time` | `absl_time` |
| `@com_google_absl//absl/types:optional` | `absl_types_optional` |
| `@com_google_absl//absl/container:flat_hash_map` | `absl_container_flat_hash_map` |
| `@com_google_absl//absl/container:flat_hash_set` | `absl_container_flat_hash_set` |
| `@com_google_absl//absl/container:node_hash_map` | `absl_container_node_hash_map` |
| `@com_google_absl//absl/container:btree` | `absl_container_btree` |
| `@com_google_absl//absl/container:inlined_vector` | `absl_container_inlined_vector` |
| `@com_google_absl//absl/random` | `absl_random_random` |
| `@com_google_absl//absl/random:bit_gen_ref` | `absl_random_bit_gen_ref` |
| `@com_google_absl//absl/synchronization` | `absl_synchronization` |
| `@com_google_absl//absl/cleanup` | `absl_cleanup` |
| `@com_google_absl//absl/debugging:failure_signal_handler` | `absl_debugging_failure_signal_handler` |
| `@com_google_absl//absl/debugging:stacktrace` | `absl_debugging_stacktrace` |
| `@com_google_absl//absl/debugging:symbolize` | `absl_debugging_symbolize` |
| `@com_google_absl//absl/algorithm` | `absl_algorithm` |
| `@com_google_absl//absl/strings` | `absl_strings` |
| `@com_google_absl//absl/strings:str_format` | `absl_strings` *(str_format is part of strings)* |
| `@com_google_absl//absl/memory` | `absl_memory` |

#### Boost

All Boost modules resolve to the same underlying `boost` dependency (due to a Meson limitation — see `eugo/dependencies/meson.build`). Use the aliased names for clarity:

| Bazel Label | Meson Variable |
|---|---|
| `@boost//:asio` | `boost_asio` |
| `@boost//:system` | `boost_system` |
| `@boost//:thread` | `boost_thread` |
| `@boost//:fiber` | `boost_fiber` |
| `@boost//:beast` | `boost_beast` |
| `@boost//:any` | `boost_any` |
| `@boost//:bimap` | `boost_bimap` |
| `@boost//:circular_buffer` | `boost_circular_buffer` |
| `@boost//:algorithm` | `boost_algorithm` |
| `@boost//:bind` | `boost_bind` |
| `@boost//:functional` | `boost_functional` |
| `@boost//:iostreams` | `boost_iostreams` |
| `@boost//:optional` | `boost_optional` |
| `@boost//:process` | `boost_process` |
| `@boost//:range` | `boost_range` |

#### Other Libraries

| Bazel Label | Meson Variable |
|---|---|
| `@com_github_spdlog//:spdlog` | `spdlog` |
| `@msgpack` | `msgpack_cxx` |
| `@com_github_google_flatbuffers//:flatbuffers` | `flatbuffers` |
| `@com_google_protobuf//:protobuf` | `protobuf` |
| `@com_github_grpc_grpc//:grpc++` | `grpc` |
| `@com_github_grpc_grpc//:grpc_opencensus_plugin` | `grpc` *(included in meta-dep)* |
| `@nlohmann_json` | `nlohmann_json` |
| `@com_github_gflags_gflags//:gflags` | `gflags` |
| `@com_github_redis_hiredis//:hiredis` | `hiredis` |
| `@com_github_redis_hiredis//:hiredis_ssl` | `hiredis_ssl` |
| `@com_github_jupp0r_prometheus_cpp//pull` | `prometheus_cpp_pull` |
| `@io_opencensus_cpp//opencensus/stats` | `opencensus_cpp_stats` |
| `@io_opencensus_cpp//opencensus/tags` | `opencensus_cpp_tags` |
| `@io_opencensus_cpp//opencensus/exporters/stats/prometheus` | `opencensus_cpp_prometheus_exporter` |
| `@io_opencensus_cpp//opencensus/exporters/stats/stdout` | `opencensus_cpp_stdout_exporter` |
| `@io_opentelemetry_cpp//api` | `opentelemetry_cpp_api` |
| `@io_opentelemetry_cpp//exporters/otlp:otlp_grpc_metric_exporter` | `opentelemetry_cpp_otlp_grpc_metric_exporter` |
| `@io_opentelemetry_cpp//sdk/src/metrics` | `opentelemetry_cpp_metrics` |

#### Special Cases

| Bazel Label | Meson Handling |
|---|---|
| `@com_google_googletest//:gtest` | See [GTest in Production Code](#gtest-in-production-code) below |
| `@com_google_googletest//:gtest_prod` | Handled by vendored stub — see below |
| `@platforms//os:linux` (in `select()`) | `if host_machine.system() == 'linux'` |

#### GTest in Production Code

Upstream Ray's Bazel build brings GTest into **production** (non-test) code via two mechanisms. Neither requires linking the real GTest library — we handle both with vendored stubs in `eugo/include/gtest/`.

**Mechanism 1: `gtest_prod` (FRIEND_TEST macro)**

~34 production headers include `<gtest/gtest_prod.h>` to use the `FRIEND_TEST` macro, which declares test classes as friends of production classes for white-box testing. This is completely benign in production — the macro just expands to a `friend class` declaration.

Our vendored `eugo/include/gtest/gtest_prod.h` is a copy of the real header. It defines only the `FRIEND_TEST` macro. No action needed — **do NOT add gtest as a Meson dependency** for targets that only use `gtest_prod`. The vendored header is found automatically via the `-I` include path in root `meson.build`.

Files using `FRIEND_TEST` (representative, not exhaustive):
- `src/ray/raylet/scheduling/cluster_resource_manager.h` (~30 FRIEND_TEST declarations)
- `src/ray/raylet/scheduling/local_resource_manager.h`
- `src/ray/raylet/scheduling/cluster_resource_scheduler.h`
- `src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.h`
- `src/ray/core_worker/core_worker.h`
- `src/ray/gcs/gcs_placement_group_manager.h`
- `src/ray/pubsub/publisher.h`, `subscriber.h`
- Many others (run `grep -rn '#include.*gtest/gtest_prod' src/ray/ --include='*.h' | grep -v /test` to see all)

**Mechanism 2: `gtest` / `gtest_main` (assertions in non-test code)**

Occasionally, a Bazel target that is NOT a test file declares a dependency on `@com_google_googletest//:gtest` or `:gtest_main`. When you encounter this:

1. **Examine the source files** to determine what GTest features are actually used.
2. If the code uses `ASSERT_TRUE`, `ASSERT_FALSE`, `ASSERT_EQ`, `EXPECT_*`, etc.:
   - Replace with standard C++ `assert()` equivalents.
   - Example: `ASSERT_FALSE(x)` → `assert(x == false)` or `RAY_CHECK(!x)`.
   - Mark the change with `@EUGO_CHANGE` annotation.
3. If only `FRIEND_TEST` is used → it's actually a `gtest_prod` case (Mechanism 1 above). No changes needed.
4. Our vendored `eugo/include/gtest/gtest.h` is an **empty stub** — any accidental `#include <gtest/gtest.h>` will compile but GTest macros like `ASSERT_*` will be undefined, causing a compile error that forces you to address the usage.

**Decision tree for Bazel targets with gtest deps:**

```
Bazel dep includes gtest or gtest_prod?
├── gtest_prod only → No action. Vendored stub handles FRIEND_TEST.
├── gtest or gtest_main →
│   ├── Source only uses FRIEND_TEST? → Actually gtest_prod. No action.
│   ├── Source uses ASSERT_*/EXPECT_*? →
│   │   ├── Replace with assert() or RAY_CHECK(). Add @EUGO_CHANGE.
│   │   └── Do NOT add gtest as a Meson dependency.
│   └── Source uses other gtest features? →
│       └── Evaluate case-by-case. Prefer eliminating the dependency.
└── In all cases: Do NOT list gtest in the Meson _dependencies array.
```

### Python Dependencies

Declared in `pyproject.toml`:

- `[project.dependencies]` — core runtime deps
- `[project.optional-dependencies]` — feature groups (`default`, `all`, `data`, `serve`, `tune`, `train`, `rllib`, etc.)

**Key EUGO difference:** Packages that upstream Ray vendors inside its Bazel build (e.g., aiohttp, colorama, psutil, setproctitle) are declared as standard pip dependencies in the `default` optional group.

---

## 11. Key Gotchas and Pitfalls

### New Source Files Not in `meson.build`

The #1 issue during syncs. Upstream adds `.cc` / `.c` files that don't exist in our Meson build. **Always run `eugo_sync.py` after syncing** to catch these. Adding a source file to the wrong library, or in the wrong dependency order, will cause link failures.

### Protobuf Schema Changes

New `.proto` files need both C++ and Python codegen targets. The Python codegen uses `eugo/utils/patching_protoc_py.sh` which patches import paths — if upstream changes proto package structure (adds, removes, or renames subdirectories under `src/ray/protobuf/`), this script **must** be updated with corresponding `sed` patches. See §8.4.1 for the full procedure, current patch list, and historical context. Failure to update the script causes `ImportError` at runtime when Python code imports the generated `_pb2.py` files.

### New External Dependencies

Upstream may add a new C++ dependency via Bazel that has no Meson equivalent configured. This manifests as missing headers or undefined symbols. Check upstream's `WORKSPACE` and `bazel/` for new external deps and add them to `eugo/dependencies/meson.build`.

### Circular Dependency Ordering in `src/ray/meson.build`

The `subdir()` order in `src/ray/meson.build` is **critical** — it resolves circular dependencies between Ray's C++ libraries. Two libraries (`stats_cpp_lib` and `object_manager_cpp_lib`) are defined inline in this file specifically to break dependency cycles. Adding new subdirectories requires understanding where they fit in this ordering.

### The Largest Files
As of this writing, the following have the largest number of lines and are most likely to have merge conflicts during syncs. Pay special attention to these:
- `@/src/ray/core_worker`
- `@/src/ray`
- `@/src/ray/protobuf`
- `@/src/ray/object_manager/plasma`
- `@/src/ray/raylet`
- `@/src/raylet/scheduling`
- `@/src/ray/util`

These are also the most critical for functionality, so careful merging is essential.

### Cython Extension Linkage

The `_raylet` extension module in `python/meson.build` is linked against all major C++ static libraries. If upstream adds a new top-level library that Cython code references, it must be added to the `dependencies:` list there.

### Python `install_subdir()` Exclusions

`python/meson.build` has extensive `exclude_*` parameters in `install_subdir('ray/')`. If upstream adds new directories that shouldn't be packaged (tests, examples, build artifacts), they need to be added to the exclusion list — but **not** directories that should be shipped. Compare against the upstream wheel to verify.

### The `@begin` / `@end` Section Markers in `meson.build`

Many `meson.build` files use `# === @begin: SectionName ===` / `# === @end: SectionName ===` markers. These are organizational but also help humans and agents understand the structure. Preserve them when editing.

### Version Script

The `_raylet` extension uses `ray_version_script.lds` to control symbol visibility (Linux only — Eugo targets Linux HPC exclusively). The upstream Bazel build is platform-conditional (macOS uses `ray_exported_symbols.lds`), but our Meson build unconditionally uses the Linux version script. If upstream changes exported symbols, `ray_version_script.lds` may need updating.

### BUILD.bazel ↔ meson.build Correspondence

**Every directory under `src/ray/` that contains a `BUILD.bazel` with production (non-test) targets MUST have a corresponding `meson.build` file.** This is a strict invariant.

When files from a subdirectory are "inlined" into a parent `meson.build` (e.g., `'scheduling/fixed_point.cc'` listed in the parent's `static_library()`), this is **incorrect** — it should instead use `subdir('scheduling/')` to delegate to a child `meson.build` that defines its own targets matching the subdirectory's `BUILD.bazel` 1:1.

**Excluded directories** (no meson.build needed):
- `*/tests/` and `*/integration_tests/` — test-only directories
- `src/ray/thirdparty/setproctitle/` — replaced by pip package
- `src/ray/core_worker/lib/java/` — Java JNI, not built in Meson

**To audit for missing meson.build files:**
```bash
find src/ray -name BUILD.bazel -exec dirname {} \; | sort | while read d; do
  [ -f "$d/meson.build" ] || echo "MISSING: $d"
done
# Then filter out test directories and intentionally excluded paths.
```

**Known production directories still needing 1:1 meson.build breakout:**
- `src/ray/protobuf/public/` — proto targets, may be inlined in `protobuf/meson.build`

### Directory Count Invariant

**The number of `meson.build` files under `src/ray/` MUST be greater than or equal to the number of `BUILD.bazel` files** (excluding test directories and intentionally excluded paths). If there are fewer `meson.build` files than `BUILD.bazel` files, some directories are missing their Meson build definitions.

```bash
# Count BUILD.bazel directories (excluding tests/excluded)
bazel_count=$(find src/ray -name BUILD.bazel -exec dirname {} \; | grep -v -E '(tests|integration_tests|thirdparty/setproctitle|lib/java)' | wc -l)

# Count meson.build directories
meson_count=$(find src/ray -name meson.build -exec dirname {} \; | wc -l)

echo "BUILD.bazel dirs: $bazel_count, meson.build dirs: $meson_count"
# meson_count >= bazel_count (after removing excluded dirs) is the invariant
```

Use `eugo_audit_targets.py` for a comprehensive per-target audit that includes this check and reports exactly which targets are missing.

---

## 12. Directory Reference

| Directory | Ownership | Description |
|-----------|-----------|-------------|
| `eugo/` | EUGO | Build infrastructure: deps, codegen helpers, vendored headers |
| `eugo/dependencies/` | EUGO | All external C++ dependency declarations |
| `eugo/utils/` | EUGO | Codegen wrappers (protoc, flatc) and post-processing scripts |
| `eugo/include/` | EUGO | Vendored headers (gtest stubs, opencensus protos) |
| `src/ray/` | MIXED | C++ source (upstream) + `meson.build` (EUGO) |
| `src/ray/protobuf/` | MIXED | Proto sources (upstream) + codegen targets (EUGO) |
| `python/` | MIXED | Python source (upstream) + `meson.build` (EUGO) |
| `python/ray/` | UPSTREAM | Pure Python Ray package |
| `bazel/` / `BUILD.bazel` | INERT | Upstream Bazel files — not used, not deleted |
| `java/` | UPSTREAM | Java bindings — not built by Meson |
| `doc/` | UPSTREAM | Documentation |
| `rllib/` | UPSTREAM | Reinforcement learning library |
| `ci/` | UPSTREAM | CI configuration (EUGO CI is external) |
| `docker/` | UPSTREAM | Docker build configs |

---

## 13. Rules for AI Coding Agents

### General Rules

1. **Read this file first** before making any changes to the repository.
2. **Understand ownership** (§7) before modifying any file — never overwrite EUGO-owned files with upstream content.
3. **Never remove or modify `@EUGO_CHANGE` annotations** unless explicitly told to do so.
4. **Always annotate** new modifications to upstream files with `@EUGO_CHANGE` markers following the convention in §5.
5. **Run `python3 eugo_sync.py`** after any change that adds or removes C/C++ source files.

### When Helping With Upstream Syncs

1. **Identify file ownership first.** Use §7 and `grep -rl "@EUGO_CHANGE"` to classify every changed file.
2. **For UPSTREAM files:** Apply changes directly.
3. **For SHARED files:** Show the diff between upstream and local, highlighting `@EUGO_CHANGE` blocks. Propose a merge that preserves all annotations.
4. **For EUGO-OWNED files:** Do not overwrite. If upstream structural changes require updates (e.g., new source files), propose additions to the relevant `meson.build`.
5. **For new source files:** Identify the correct `meson.build` and the correct position in the dependency graph before adding.

### When Modifying Build Files

1. **Replicate Bazel 1:1 first** — create one Meson target per `ray_cc_library()` in `BUILD.bazel` (see §3). Do NOT consolidate multiple Bazel targets into one Meson library during initial translation.
2. **Use the correct pattern** from §2 for each target type (static library, header-only, FlatBuffers, Protobuf, gRPC).
3. **Header-only targets must NOT use `static_library()`** — use `declare_dependency()` only (Pattern 2 in §2). Creating a `static_library()` for header-only targets causes duplicate symbols.
4. **Map ALL dependencies** — every Bazel `deps` entry must have a Meson equivalent. Use the mapping table in §10. Missing deps cause linker errors that are hard to trace.
5. **Map ALL source files** — every `.cc` file in Bazel `srcs` must appear in the Meson `static_library()`. Missing files mean missing functionality.
6. **Validate source lists and deps** against upstream `BUILD.bazel` using the procedure in §3.
7. **Respect `subdir()` ordering** in `src/ray/meson.build` — it encodes the dependency graph.
8. **Add new external deps** to `eugo/dependencies/meson.build`, not inline in component build files.
9. **Use the codegen kwargs** from `eugo/utils/meson.build` for new proto/flatbuffers targets.
10. **Run `python3 eugo_sync.py`** after adding/removing source files (see §4 for what the output means).
11. **Test with `eugo_meson_compile.sh`** after build file changes.
12. **Every `BUILD.bazel` directory needs a `meson.build`** — if you find files from a subdirectory inlined in a parent `meson.build`, break them out into their own `meson.build` in the correct subdirectory using `subdir()`. See §11 for the full rule and exceptions.
13. **Handle gtest deps correctly** — follow the decision tree in §10 "GTest in Production Code". Never add gtest as a Meson dependency. For `gtest_prod` (FRIEND_TEST), our vendored stub handles it. For `gtest`/`gtest_main`, examine sources and replace assertions with `assert()` or `RAY_CHECK()`.
14. **Clean up empty directories with orphaned `meson.build` files** — if a directory contains only a `meson.build` and no source files, follow the assessment procedure in §8.10 to determine whether to delete, move, or inline the targets.
15. **Mandatory target-count audit** — before considering a `meson.build` complete, count the production `ray_cc_library()` targets in the corresponding `BUILD.bazel` and compare against the `# === @begin` blocks in the `meson.build`. They MUST match 1:1. If the `meson.build` has fewer targets, it is INCOMPLETE — identify the missing Bazel targets and create Meson equivalents. This is the #1 source of breakout errors: consolidating multiple Bazel targets into one Meson library. Use the full validation procedure in §3.
16. **Verify all deps exist** — for every `*_cpp_lib_dep` referenced in a `_dependencies` list, verify it is actually defined in a `meson.build` that is processed BEFORE this one. A reference to a nonexistent dep is a build error. Use `grep -rn 'foo_cpp_lib_dep =' src/ray/**/meson.build` to verify.
17. **Unavailable / Circular dep documentation** — when a Bazel dep cannot be added because the target is defined in a `meson.build` processed later (ordering constraint), it MUST be represented as a **commented-out dependency inline** in the `_dependencies` list using the standard format:
    ```meson
    # <meson_variable_name> is NOT AVAILABLE (processed after): //<bazel_label>
    ```
    For example:
    ```meson
    # raylet_metrics_cpp_lib_dep is NOT AVAILABLE (processed after): //src/ray/raylet:metrics
    # object_manager_grpc_client_manager_cpp_lib_dep is NOT AVAILABLE (processed after): //src/ray/object_manager:object_manager_grpc_client_manager and is resolved at final link time via `<>` library.
    ```
    The comment must be placed at the **exact position** where the dep would appear if it were available (i.e., matching the Bazel ordering). This ensures: (a) every Bazel dep is accounted for — nothing is silently dropped, (b) the Meson variable name is documented for future use if ordering changes, and (c) auditing against Bazel is trivial — every dep line maps 1:1. Do NOT use separate `IMPORTANT` or `NOTE` block comments outside the dependencies list — the inline commented-out entry IS the documentation. These deps are resolved at final link time via the `_raylet` shared library.
18. **Directory count invariant** — the number of `meson.build` files under `src/ray/` MUST be greater than or equal to the number of `BUILD.bazel` files (excluding test directories and intentionally excluded paths). If there are fewer `meson.build` files than `BUILD.bazel` files, some directories are missing their Meson build definitions. See §11 for the full invariant and the audit command. Use `python3 eugo_audit_targets.py` for a comprehensive automated check.
19. **Maintain Bazel target ordering** — `# === @begin` blocks in a `meson.build` MUST appear in the same top-to-bottom order as the corresponding `ray_cc_library()` targets in `BUILD.bazel`. The only permitted deviation is when a target's dependency is also defined in the same file and must therefore be placed before its consumer (Meson define-before-use). Document any such deviation with a comment. **When ANY deviation from BUILD.bazel order exists**, the `meson.build` file MUST have an `# IMPORTANT` comment block at the top explaining that ordering respects the internal dependency graph (see §3 "Target Ordering Rule" for the full explanation, example, and required header format).
20. **Group dependencies with section comments** — every `_dependencies` list must separate package-managed deps (other Meson library targets) from Eugo-managed deps (external dependencies from `eugo/dependencies/meson.build`) using `# Package-managed` and `# Eugo-managed` section comments, with a blank line between groups. This makes it immediately clear which deps are internal project libraries vs. external third-party libraries. Example:
    ```meson
    foo_cpp_lib_dependencies = [
        # Package-managed
        bar_cpp_lib_dep,
        baz_cpp_lib_dep,

        # Eugo-managed
        absl_time,
        protobuf,
    ]
    ```
21. **List dependencies in Bazel order** — within each group (`# Package-managed` and `# Eugo-managed`), dependencies MUST appear in the same order as they appear in the corresponding Bazel `deps` list. This 1:1 ordering makes manual cross-referencing fast and ensures no deps are accidentally skipped during translation and validation.

### When Modifying C++ Code

1. **Wrap changes** in `@EUGO_CHANGE: @begin: <reason>` / `@EUGO_CHANGE: @end` blocks.
2. **Keep the original code** as a comment inside the block when practical.
3. If the fix is no longer needed, **promote to `@NO_CHANGE`** rather than deleting the annotation.
4. **Never modify upstream code** without an `@EUGO_CHANGE` annotation.

### When Modifying Python Code / Config

1. **`pyproject.toml` is EUGO-owned** — it uses `meson-python`, not setuptools. Any dependency changes should go here, not in `setup.py`.
2. Keep `pyproject.toml` dependencies in sync with upstream's `setup.py` and `requirements.txt`.
3. If upstream adds new vendored thirdparty packages, add them as pip dependencies in the appropriate `[project.optional-dependencies]` group instead.

### When Debugging Build Failures

1. **Missing symbols at link time** → check if a new source file needs to be added to a `meson.build`, or a new dependency is needed in `eugo/dependencies/meson.build`.
2. **Missing headers** → check if a new external dependency needs to be declared, or if `add_project_arguments('-I' + ...)` in root `meson.build` needs an update.
3. **Proto/codegen errors** → check `eugo/utils/meson.build` for codegen kwargs and `eugo/utils/patching_protoc_py.sh` for Python import patching.
4. **Circular dependency build errors** → the ordering in `src/ray/meson.build` likely needs adjustment. Read the comments there carefully.
5. **Wheel content mismatch** → check `install_subdir()` exclusions in `python/meson.build`.

---

## 14. Step 2: Shared Library Consolidation

> **Prerequisite:** Step 1 (1:1 Bazel-to-Meson translation with individual static libraries) MUST be complete and tested before starting Step 2. Do NOT begin this work until the build compiles, the wheel builds, and `eugo_sync.py` is clean.

### Goal

Replace the all (~30+) individual `static_library()` targets under `src/ray/` with a **single `shared_library()`** that compiles all production C++ source files together. This enables:

- **Link-Time Optimization (LTO)** across the entire C++ codebase (not just within individual static libraries)
- **Reduced build complexity** — eliminates the intricate `subdir()` ordering, circular dependency workarounds, and hundreds of `_dependencies` / `_dep` / `_lib` variable declarations
- **Faster linking** — one link step instead of many archive + final link steps
- **Better dead code elimination** — the linker can see all symbols at once

### Architecture After Step 2

```
meson.build (root)
├── eugo/
│   ├── dependencies/meson.build  # External C++ deps (unchanged)
│   ├── utils/meson.build         # Codegen helpers (unchanged)
│   └── include/                  # Vendored headers (unchanged)
├── src/
│   ├── proto/                    # .proto and .fbs source files (unchanged)
│   └── ray/
│       ├── protobuf/meson.build  # Proto/FBS codegen custom_targets (kept, but NO static_library)
│       └── meson.build           # Single shared_library('ray_cpp', ...) with ALL .cc files
└── python/
    └── meson.build               # _raylet links against ray_cpp shared lib instead of static libs
```

### Pre-Consolidation Notes

#### 14.1. Duplicate Filenames — Not a Problem

Meson encodes the **full relative source path** into each object filename by replacing `/` with `_`. All object files land in a target-specific subdirectory (e.g., `libray_cpp.so.p/`), so files with the same basename from different directories produce distinct object files:

- `object_manager/common.cc` → `libray_cpp.so.p/object_manager_common.cc.o`
- `core_worker/common.cc` → `libray_cpp.so.p/core_worker_common.cc.o`
- `core_worker/task_execution/common.cc` → `libray_cpp.so.p/core_worker_task_execution_common.cc.o`
- `rpc/common.cc` → `libray_cpp.so.p/rpc_common.cc.o`

Ray's `src/ray/` tree actually has 4 files named `common.cc` (`object_manager/`, `core_worker/`, `core_worker/task_execution/`, `rpc/`) — all compile correctly with no collision.

**The only theoretical edge case** is two source paths that differ only by replacing a directory separator with an underscore (e.g., `sub1/common.cc` and `sub1_common.cc`), which would produce identical object filenames. This pattern does not exist in Ray's codebase.

Headers are also fine — Ray uses fully-qualified includes (`ray/util/foo.h`, `src/ray/util/foo.h`) rather than bare `foo.h`, so header name collisions don't cause shadowing.

#### 14.2. Include Directories — Not Needed

Each `static_library()` in Step 1 implicitly adds its own source directory to the include path. The Step 2 `shared_library()` does **not** implicitly add any source subdirectory — but this is not an issue because the root `meson.build` already sets:

```meson
add_project_arguments('-I' + project_source_root() / 'src', ...)
add_project_arguments('-I' + project_source_root(), ...)
```

These two flags cover every header under `src/ray/` via their fully-qualified paths (`src/ray/util/foo.h`, etc.), which is exactly how Ray's includes are written. No explicit `include_directories()` list is needed for the shared library.

### Meson Pattern: Single Shared Library

After the audit passes, the consolidation replaces all `static_library()` + `declare_dependency()` patterns with one `shared_library()`:

```meson
# src/ray/meson.build (Step 2)

# --- Codegen (kept from Step 1, but produces custom_targets only, no static libs) ---
subdir('protobuf/')    # Defines *_proto_cpp, *_proto_cpp_grpc custom_targets
subdir('flatbuffers/') # Defines *_cpp_fbs custom_targets

# --- All production source files ---
ray_cpp_sources = files(
    'common/status.cc',
    'common/task_spec.cc',
    'common/memory_monitor.cc',
    # ... every production .cc file from all subdirectories ...
    'util/event.cc',
    'util/logging.cc',
    'gcs/gcs_server.cc',
    'raylet/main.cc',
    'core_worker/core_worker.cc',
    # ... etc ...
)

~~ @begin: # --- Include directories for all production headers ---~~
ray_cpp_include_dirs = include_directories(
    '.',           # src/ray/ itself
    'common/',
    'common/cgroup2/',
    'gcs/',
    'gcs/actor/',
    'object_manager/',
    'object_manager/plasma/',
    'pubsub/',
    'raylet/',
    'raylet/scheduling/',
    'rpc/',
    'util/',
    # ... every directory containing production headers ...
)
~~ @end ~~

# --- All external + codegen dependencies ---
ray_cpp_dependencies = [
    # Codegen outputs (custom_target results, not static libs)
    common_proto_cpp_dep,
    gcs_proto_cpp_dep,
    gcs_service_proto_cpp_grpc_dep,
    # ... all proto/fbs codegen declare_dependency() targets ...

    # External deps (from eugo/dependencies/)
    absl_base_core_headers,
    absl_time,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_synchronization,
    boost_asio,
    boost_fiber,
    boost_thread,
    flatbuffers,
    grpc,
    hiredis,
    hiredis_ssl,
    msgpack_cxx,
    nlohmann_json,
    opencensus_cpp_stats,
    opencensus_cpp_tags,
    opencensus_cpp_prometheus_exporter,
    opencensus_cpp_stdout_exporter,
    prometheus_cpp_pull,
    protobuf,
    spdlog,
    threads,
    # ... all needed external deps ...
]

# --- Single shared library ---
ray_cpp_lib = shared_library(
    'ray_cpp',
    ray_cpp_sources,
    include_directories: ray_cpp_include_dirs,
    dependencies: ray_cpp_dependencies,
    install: true,
    gnu_symbol_visibility: 'hidden',  # Control exported symbols explicitly # @TODO: unsure about symbols visibility
)

ray_cpp_lib_dep = declare_dependency(
    link_with: [ray_cpp_lib],
    include_directories: ray_cpp_include_dirs,
    dependencies: ray_cpp_dependencies,
)
```

### Changes to _raylet Extension Module

The `_raylet` extension in `python/meson.build` simplifies from many dependencies to one:

```meson
# python/meson.build (Step 2)
py.extension_module(
    '_raylet',
    ['ray/_raylet.pyx'],
    install: true,
    subdir: 'ray/',
    dependencies: [ray_cpp_lib_dep],  # Single dep replaces ~10+ static lib deps
    # Eugo is Linux-only; upstream Bazel is platform-conditional (macOS uses ray_exported_symbols.lds)
    link_args: ['-Wl,--version-script=' + meson.project_source_root() / 'src/ray/ray_version_script.lds'],
    override_options: ['cython_language=cpp']
)
```

### Changes to Codegen Targets

Proto and FlatBuffers code generation stays the same (custom_targets producing `.pb.cc`, `.grpc.pb.cc`, `_generated.h`), but the downstream pattern changes:

**Step 1 (current):** codegen → `declare_dependency` (virtual) → `static_library` → `declare_dependency` (consumer)

**Step 2 (new):** codegen → `declare_dependency` (virtual, kept for generated header visibility) → feeds directly into `ray_cpp_dependencies`

The `static_library()` wrappers around proto codegen outputs are removed. The generated `.pb.cc` and `.grpc.pb.cc` files become additional sources in the shared library, or their `declare_dependency` wrappers (which carry the generated headers) are listed as dependencies so the compiler can find the generated headers.

### Validation Procedure for Step 2

1. **Symbol completeness** — link the shared library. Any missing source file produces undefined symbol errors.
2. **eugo_sync.py** — must still report "No raptors found!" (all `.cc` files referenced in the single `shared_library()` sources list).
3. **Wheel comparison** — the output wheel must produce identical runtime behavior to the Step 1 wheel.
4. **LTO verification** — build with `-Db_lto=true` and verify the shared library is smaller than the sum of all static libraries.

### Rules for AI Coding Agents (Step 2 Specific)

1. **Do NOT start Step 2 until Step 1 is complete and tested.** Step 1 is the prerequisite — the 1:1 Bazel mapping must build and pass validation first.
2. **Keep all codegen `custom_target()` definitions** — proto/fbs generation does not change in Step 2, only the downstream consumption pattern changes.
3. **Do NOT delete the Step 1 `meson.build` files until the shared library builds clean.** Work in a new branch and keep Step 1 as a fallback.
4. **Every `.cc` file currently in any `static_library()` must appear in `ray_cpp_sources`.** Missing a file means missing symbols. Cross-reference against `eugo_sync.py` output.
5. **Test-only files, Java JNI files, and `thirdparty/setproctitle/` remain excluded** — same exclusion rules as Step 1.
6. **The `_raylet` version script still applies** — use `ray_version_script.lds` unconditionally (Eugo targets Linux only). Symbol visibility control is even more important with a shared library.

---

## 15. Lessons Learned from Audit

This section documents recurring errors found during the comprehensive 1:1 Bazel-to-Meson audit. These are the most common mistakes — read this section before making any changes to `meson.build` files.

### 15.1. Define-Before-Use Ordering Violations

**Error pattern:** A `meson.build` file lists target A before target B, but A depends on B. Meson requires that B be defined (assigned to a variable) before A can reference it.

**Example:** In `src/ray/common/cgroup2/meson.build`, `noop_cgroup_manager_cpp_lib` (position 1) depended on `cgroup_driver_interface_cpp_lib_dep` (position 4). The fix was to move `cgroup_driver_interface_cpp_lib` to position 1.

**Rule:** When a target in the same `meson.build` file depends on another target also in that file, the dependency MUST appear first — even if this deviates from the BUILD.bazel order. **Every file with such deviations MUST have an `# IMPORTANT:` header comment** explaining the reordering rationale (see §3 "Target Ordering Rule").

### 15.2. `alwayslink = 1` Requires `link_whole:` Not `link_with:`

**Error pattern:** A Bazel target has `alwayslink = 1` (or `alwayslink = True`), but the Meson translation uses `link_with:` in the consumer. This silently drops unused symbols from the static library, causing missing symbol errors at runtime or during dynamic loading.

**Rule:** When a Bazel target has `alwayslink = 1`, the **consumer** of that library in Meson must use `link_whole:` instead of `link_with:`. This ensures all object files in the static library are linked, even if they contain no symbols directly referenced by the consumer. The `_raylet` extension module uses this for `exported_internal_cpp_lib`.

### 15.3. Umbrella Dependencies Must Be Disaggregated

**Error pattern:** A convenience "umbrella" dependency like `ray_common_cpp_lib_dep` (which bundles ~20 individual libraries) is used in place of the specific individual dependencies that the Bazel target actually lists. This:
- Massively over-links (pulling in 20 deps when only 3 are needed)
- Makes it impossible to audit 1:1 against Bazel
- Hides missing deps (the umbrella masks them)

**Rule:** Never use umbrella/aggregate dependencies in individual target `_dependencies` lists. Translate each Bazel `deps` entry to its exact Meson equivalent. The umbrella target itself is fine to *define* (as `ray_common_cpp_lib_dep` in `src/ray/common/meson.build`), but it should only be consumed by targets whose Bazel equivalent actually depends on `:ray_common`.

### 15.4. Dead Code Convenience Aggregates

**Error pattern:** A `meson.build` defines a convenience aggregate (e.g., `gcs_service_rpc_cpp_lib` that bundles multiple proto deps) that has zero consumers — nothing in any other `meson.build` references it.

**Rule:** If a target has zero consumers (verified by `grep -rn 'target_name' src/ray/**/meson.build`), delete it. Dead targets mislead readers and accumulate during syncs. Before deleting, verify it truly has no consumers across the entire build tree.

### 15.5. `absl/base` vs `absl/base:core_headers`

**Error pattern:** The Bazel dep `@com_google_absl//absl/base` (the full base library, including `absl::base`) is incorrectly mapped to `absl_base_core_headers` (which is only `absl::base_internal_headers` / `absl::core_headers`).

**Rule:** These are two distinct Bazel targets with different Meson variables:
- `@com_google_absl//absl/base` → `absl_base` (full base library)
- `@com_google_absl//absl/base:core_headers` → `absl_base_core_headers` (headers only)

Both are defined in `eugo/dependencies/meson.build`. Always check the exact Bazel label — `:core_headers` is a sub-target of `absl/base`, not the same thing.

### 15.6. Proto Library Deps Are Package-Managed, Not Eugo-Managed

**Error pattern:** A generated proto library dep (e.g., `gcs_proto_cpp_lib_dep`) is listed in the `# Eugo-managed` section of a `_dependencies` list, alongside external deps like `protobuf` and `grpc`.

**Rule:** Proto/gRPC codegen libraries (`*_proto_cpp_lib_dep`, `*_proto_cpp_grpc_lib_dep`) are **package-managed** — they are built by the Meson project from `.proto` files, not provided by external system packages. They belong in the `# Package-managed` section.

### 15.7. Missing Python Proto Codegen Targets

**Error pattern:** A `.proto` file has C++ codegen targets (Pattern 4/5 from §2) but is missing its Python codegen target (Pattern 6). This causes `ImportError` at runtime when Python code tries to `import` the generated `_pb2.py` file.

**Rule:** For every `.proto` file that has a Bazel `*_py_proto` target, there MUST be a corresponding `custom_target()` using `proto_py_default_kwargs` in the `meson.build`. Check the Bazel `BUILD.bazel` for `py_proto_library()` targets to find all needed Python codegen. Pay special attention to proto files in subdirectories (e.g., `protobuf/public/`) — these are often missed.

### 15.8. `@original` Annotation Format

**Error pattern:** The `@begin` line uses `(original: name)` instead of the correct format `(@original: name)`.

**Rule:** The correct format is always `(@original: <exact_bazel_target_name>)` with the `@` prefix. Example: `# === @begin: foo_cpp_lib (@original: foo) ===`. The `@original` tag maps the Meson target name back to the Bazel target name for cross-referencing.

### 15.9. Orphaned `meson.build` Files

**Error pattern:** A subdirectory contains only a `meson.build` file with no source files (`.cc`, `.h`, `.proto`), and no `subdir()` call in any parent references it. The file is dead code.

**Rule:** Follow the assessment procedure in §8.10. If the file is not referenced by any `subdir()` and all its targets are either dead or defined elsewhere, delete the file and its empty directory. Common sources: directories created during initial translation that were later reorganized.

### 15.10. `@skip` Annotations for Excluded Bazel Targets

**Error pattern:** A `meson.build` file omits a Bazel target (e.g., a fake/test-only `ray_cc_library`) without documenting the omission. During audits, this looks like a missing target.

**Rule:** When a Bazel target is intentionally excluded from the Meson build (test fakes, test utilities, etc.), add a `# @skip: <meson_name> (@original: <bazel_name>) — <reason>` comment at the position where the target would appear. This documents that the omission is intentional and prevents audit false positives. Example:
```meson
# @skip: fake_raylet_client_cpp_lib (@original: fake_raylet_client) — test-only fake, not needed in Meson
```

---
