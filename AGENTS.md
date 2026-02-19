# AGENTS.md — Eugo Ray-Meson Fork

> **Eugo — The Future of Supercomputing**
>
> This repository is a **public Meson-based fork** of [ray-project/ray](https://github.com/ray-project/ray) maintained by Eugo, a high-performance computing company.
> We replace Ray's Bazel build system with [Meson](https://mesonbuild.com/) and make targeted code modifications for HPC environments including GPU cluster scheduling (CUDA), InfiniBand/high-speed networking, and general HPC packaging and deployment.

---

## Table of Contents

1. [Repository Overview](#1-repository-overview)
2. [Build System Architecture](#2-build-system-architecture)
3. [EUGO Change Annotation Convention](#3-eugo-change-annotation-convention)
4. [Upstream Sync Process](#4-upstream-sync-process)
5. [Files and Ownership Rules](#5-files-and-ownership-rules)
6. [Common Sync Scenarios](#6-common-sync-scenarios)
7. [Build and Validation](#7-build-and-validation)
8. [Dependency Management](#8-dependency-management)
9. [Key Gotchas and Pitfalls](#9-key-gotchas-and-pitfalls)
10. [Directory Reference](#10-directory-reference)
11. [Rules for AI Coding Agents](#11-rules-for-ai-coding-agents)

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
│       ├── protobuf/ → thirdparty/ → util/ → stats/ → raylet/format/ → common/ → rpc/
│       ├── Then: stats_cpp_lib (inline, breaks circular dep)
│       ├── Then: object_manager/ → pubsub/ → raylet_client/ → gcs/
│       ├── Then: object_manager_cpp_lib (inline, breaks circular dep)
│       └── Then: raylet/ → core_worker/ → internal/
└── python/                       # Python package
    ├── ray/meson.build           # _version.py generation (git SHA injection)
    ├── meson.build               # _raylet Cython extension + pure Python install + dashboard
    └── ray/dashboard/client/     # npm ci + npm run build
```

### C++ Library Pattern

Every C++ component in `src/ray/` follows this consistent pattern:

```meson
# 1. Dependencies list
foo_cpp_lib_dependencies = [
    bar_cpp_lib_dep,      # Package-managed (sibling project libs)
    absl_time, protobuf,  # Eugo-managed (from eugo/dependencies/)
    threads               # System-managed
]

# 2. Static library
foo_cpp_lib = static_library('foo_cpp_lib',
    ['file1.cc', 'file2.cc'],
    install: false,
    dependencies: foo_cpp_lib_dependencies
)

# 3. Dependency declaration for downstream consumers
foo_cpp_lib_dep = declare_dependency(
    link_with: [foo_cpp_lib],
    dependencies: foo_cpp_lib_dependencies
)
```

### Build Scripts

| Script | Purpose |
|--------|---------|
| `eugo_bootstrap.sh` | One-time environment setup (Node.js, directories) |
| `eugo_meson_setup.sh` | `meson setup --reconfigure eugo_build` with warning suppression flags |
| `eugo_meson_compile.sh` | `meson compile -C eugo_build` |
| `eugo_pip3_wheel.sh` | `pip3 wheel .` using `meson-python` backend (build dir: `eugo_build_whl`) |
| `eugo_sync.py` | Verification: finds C/C++ source files in `src/` not referenced by any `meson.build` |

---

## 3. EUGO Change Annotation Convention

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

## 4. Upstream Sync Process

We track `ray-project/ray` `master` (nightly). Syncing is a combination of automated scripts and manual cherry-pick/merge.

### Sync Workflow

```
1. Fetch upstream master
2. Identify changed files (automated diff)
3. Classify files by ownership (see §5)
4. Apply upstream changes to UPSTREAM-owned files
5. For SHARED files, manually merge — preserve @EUGO_CHANGE blocks
6. Run eugo_sync.py to detect new untracked source files
7. Update meson.build files for any new/removed source files
8. Build and validate (see §7)
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

## 5. Files and Ownership Rules

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

## 6. Common Sync Scenarios

### 6.1. New C++ Source Files Added Upstream

**Symptom:** `eugo_sync.py` reports files that aren't tracked in any `meson.build`.

**Resolution:**
1. Identify which library the file belongs to (check the upstream `BUILD.bazel` for the target).
2. Add the file to the appropriate `static_library()` call in `src/ray/<component>/meson.build`.
3. If it's a new component entirely, create a new `meson.build` following the standard library pattern (§2) and add a `subdir()` call in the parent `meson.build` at the correct position in the dependency ordering.

### 6.2. C++ Source Files Removed Upstream

**Symptom:** Meson build fails with "file not found".

**Resolution:**
1. Remove the file reference from the relevant `meson.build`.
2. Remove the file itself.

### 6.3. New External Dependency Added Upstream

**Symptom:** Meson build fails with undefined symbols or missing headers.

**Resolution:**
1. Add the dependency in `eugo/dependencies/meson.build` using `dependency()` with the appropriate method (`cmake`, `pkg-config`, etc.).
2. Add the dependency variable to the relevant library's `_dependencies` list.

### 6.4. Protobuf Schema Changes

**Symptom:** Build fails during codegen or downstream compilation.

**Resolution:**
1. If new `.proto` files: add a `custom_target()` in `src/ray/protobuf/meson.build` (for C++) and the Python proto `meson.build` using the kwargs from `eugo/utils/meson.build`.
2. If modified `.proto` files: usually just rebuilds automatically.
3. If deleted `.proto` files: remove the corresponding `custom_target()` entries.
4. Check `eugo/utils/patching_protoc_py.sh` if Python import paths changed.

### 6.5. FlatBuffers Schema Changes

**Symptom:** Missing `*_generated.h` files.

**Resolution:**
Add/remove `custom_target()` entries in `src/ray/raylet/format/meson.build` using `cpp_fbs_default_kwargs`.

### 6.6. New Python Dependencies

**Symptom:** Import errors at runtime or missing packages.

**Resolution:**
1. Check upstream `setup.py` and `requirements.txt` for new entries.
2. Add them to `pyproject.toml` — under `[project.dependencies]` for core deps or `[project.optional-dependencies]` for optional groups.
3. The `@EUGO_CHANGE` annotation in `pyproject.toml` reminds you to keep these in sync.

### 6.7. Cython Changes (`_raylet.pyx`)

**Symptom:** Compilation errors in the `_raylet` extension module.

**Resolution:**
1. Accept the upstream `.pyx` / `.pxd` changes.
2. If new C++ libraries are referenced from Cython, add them to the `dependencies:` list in `python/meson.build`'s `py.extension_module('_raylet', ...)` call.

### 6.8. Upstream Modifies a File Containing `@EUGO_CHANGE`

**Resolution:**
1. **Do not blindly overwrite.** This is a SHARED file.
2. Manually merge — apply the upstream changes around the `@EUGO_CHANGE` blocks.
3. Verify the `@EUGO_CHANGE` is still needed:
   - If the upstream change fixes the same issue → promote to `@NO_CHANGE` and use upstream code.
   - If the upstream change conflicts → adapt the EUGO change and update the annotation reason.
   - If the upstream change is orthogonal → merge normally, keep annotation intact.

### 6.9. Dashboard (JavaScript) Changes

**Symptom:** Dashboard build fails or shows stale UI.

**Resolution:**
The Meson build runs `npm ci && npm run build` in `python/ray/dashboard/client/`. Upstream changes to `package.json` / `package-lock.json` should be accepted. The `meson.build` in that directory should not need changes unless the build command or output directory changes.

---

## 7. Build and Validation

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

---

## 8. Dependency Management

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

### Python Dependencies

Declared in `pyproject.toml`:

- `[project.dependencies]` — core runtime deps
- `[project.optional-dependencies]` — feature groups (`default`, `all`, `data`, `serve`, `tune`, `train`, `rllib`, etc.)

**Key EUGO difference:** Packages that upstream Ray vendors inside its Bazel build (e.g., aiohttp, colorama, psutil, setproctitle) are declared as standard pip dependencies in the `default` optional group.

---

## 9. Key Gotchas and Pitfalls

### New Source Files Not in `meson.build`

The #1 issue during syncs. Upstream adds `.cc` / `.c` files that don't exist in our Meson build. **Always run `eugo_sync.py` after syncing** to catch these. Adding a source file to the wrong library, or in the wrong dependency order, will cause link failures.

### Protobuf Schema Changes

New `.proto` files need both C++ and Python codegen targets. The Python codegen uses `eugo/utils/patching_protoc_py.sh` which patches import paths — if upstream changes proto package structure, this script may need updating.

### New External Dependencies

Upstream may add a new C++ dependency via Bazel that has no Meson equivalent configured. This manifests as missing headers or undefined symbols. Check upstream's `WORKSPACE` and `bazel/` for new external deps and add them to `eugo/dependencies/meson.build`.

### Circular Dependency Ordering in `src/ray/meson.build`

The `subdir()` order in `src/ray/meson.build` is **critical** — it resolves circular dependencies between Ray's C++ libraries. Two libraries (`stats_cpp_lib` and `object_manager_cpp_lib`) are defined inline in this file specifically to break dependency cycles. Adding new subdirectories requires understanding where they fit in this ordering.

### Cython Extension Linkage

The `_raylet` extension module in `python/meson.build` is linked against all major C++ static libraries. If upstream adds a new top-level library that Cython code references, it must be added to the `dependencies:` list there.

### Python `install_subdir()` Exclusions

`python/meson.build` has extensive `exclude_*` parameters in `install_subdir('ray/')`. If upstream adds new directories that shouldn't be packaged (tests, examples, build artifacts), they need to be added to the exclusion list — but **not** directories that should be shipped. Compare against the upstream wheel to verify.

### The `@begin` / `@end` Section Markers in `meson.build`

Many `meson.build` files use `# === @begin: SectionName ===` / `# === @end: SectionName ===` markers. These are organizational but also help humans and agents understand the structure. Preserve them when editing.

### Version Script

The `_raylet` extension uses a linker version script (`ray_version_script.lds`) to control symbol visibility. If upstream changes exported symbols, this file may need updating.

---

## 10. Directory Reference

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

## 11. Rules for AI Coding Agents

### General Rules

1. **Read this file first** before making any changes to the repository.
2. **Understand ownership** (§5) before modifying any file — never overwrite EUGO-owned files with upstream content.
3. **Never remove or modify `@EUGO_CHANGE` annotations** unless explicitly told to do so.
4. **Always annotate** new modifications to upstream files with `@EUGO_CHANGE` markers following the convention in §3.
5. **Run `python3 eugo_sync.py`** after any change that adds or removes C/C++ source files.

### When Helping With Upstream Syncs

1. **Identify file ownership first.** Use §5 and `grep -rl "@EUGO_CHANGE"` to classify every changed file.
2. **For UPSTREAM files:** Apply changes directly.
3. **For SHARED files:** Show the diff between upstream and local, highlighting `@EUGO_CHANGE` blocks. Propose a merge that preserves all annotations.
4. **For EUGO-OWNED files:** Do not overwrite. If upstream structural changes require updates (e.g., new source files), propose additions to the relevant `meson.build`.
5. **For new source files:** Identify the correct `meson.build` and the correct position in the dependency graph before adding.

### When Modifying Build Files

1. **Follow the library pattern** in §2 for new C++ components.
2. **Respect `subdir()` ordering** in `src/ray/meson.build` — it encodes the dependency graph.
3. **Add new external deps** to `eugo/dependencies/meson.build`, not inline in component build files.
4. **Use the codegen kwargs** from `eugo/utils/meson.build` for new proto/flatbuffers targets.
5. **Test with `eugo_meson_compile.sh`** after build file changes.

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
