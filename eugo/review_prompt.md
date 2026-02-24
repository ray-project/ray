# Meson Build Review Prompt Template

> **Purpose:** Systematic per-directory review of every `meson.build` file against its corresponding `BUILD.bazel` to catch errors before compilation.
>
> **Usage:** Instantiate this template once per `meson.build` file. Replace `{{MESON_PATH}}` and `{{BAZEL_PATH}}` with actual paths. Feed the instantiated prompt to an LLM along with the contents of both files and the full AGENTS.md.

---

## Prompt

You are an expert build system engineer reviewing a Meson build file in a fork of [ray-project/ray](https://github.com/ray-project/ray) that replaces Bazel with Meson. Your job is to audit the Meson file for correctness, completeness, and compliance with the project's conventions documented in AGENTS.md.

### Files Under Review

- **Meson file:** `{{MESON_PATH}}`
- **Bazel file:** `{{BAZEL_PATH}}`

Read both files in their entirety before beginning. Also read AGENTS.md (attached or at the repo root).

If the Meson file is under `src/ray/` and the corresponding BUILD.bazel has production (non-test) `ray_cc_library()` targets, perform ALL checks below. If the Meson file is a codegen-only file (proto/flatbuffers), infrastructure file (eugo/), or Python packaging file (python/), adapt the checks as noted.

---

### Phase 1: Target Inventory Audit

**Goal:** Verify 1:1 correspondence between Bazel targets and Meson targets.

1. **Count production Bazel targets.** List every `ray_cc_library()` in the BUILD.bazel that is NOT a test target (exclude names containing `test`, `mock`, `fake`, or targets inside `cc_test()` / `ray_cc_test()` blocks). Also list `flatbuffer_cc_library()`, `cc_proto_library()`, `cc_grpc_library()`, and `proto_library()` targets.

2. **Count Meson targets.** List every `# === @begin:` block in the meson.build. Each block = 1 target.

3. **Compare.** Every production Bazel target MUST have exactly one corresponding Meson target. Report:
   - **Missing in Meson:** Bazel targets with no `@begin` block
   - **Extra in Meson:** `@begin` blocks with no Bazel counterpart
   - **Intentionally skipped:** Targets documented with `# NOTE: @skip:` — verify the skip reason is valid

4. **Check `@original` annotations.** Every `@begin` block should have `(@original: <bazel_name>)` matching the Bazel target name. Report mismatches.

---

### Phase 2: Per-Target Deep Audit

For EACH production Bazel target, perform these checks against its Meson counterpart:

#### 2A. Pattern Classification

| Bazel | Expected Meson Pattern | Reference |
|-------|----------------------|-----------|
| `ray_cc_library` with `srcs` (.cc files) | Pattern 1: static_library (AGENTS.md §2) | 3 variables: `_dependencies`, `_lib`, `_dep` |
| `ray_cc_library` with only `hdrs` | Pattern 2: header-only (AGENTS.md §2) | `_dep` only, comment `# Header-only`, NO static_library |
| `flatbuffer_cc_library` | Pattern 3: FlatBuffers codegen | custom_target + header-only dep |
| `cc_proto_library` | Pattern 4: C++ Protobuf | custom_target → virtual dep → static_library → consumer dep |
| `cc_grpc_library` | Pattern 5: C++ gRPC | Same as Pattern 4 + grpc dep |
| `proto_library` (Python) | Pattern 6: Python Protobuf | custom_target only |

**Check:** Is the correct pattern used? Report if:
- A header-only target uses `static_library()` (WRONG — causes duplicate symbols)
- A target with `.cc` sources uses header-only pattern (WRONG — missing compiled code)

#### 2B. Source Files

For Pattern 1 (static library) targets:
- List every `.cc` file in the Bazel `srcs` field
- Verify each appears in the Meson `static_library()` call
- Report any missing or extra `.cc` files

For Pattern 2 (header-only) targets:
- Verify `.h` files appear in `declare_dependency(sources: [...])`
- Or verify the target has `# Header-only` comment

#### 2C. Dependencies — Completeness

For each Bazel `deps` entry, verify a corresponding Meson dependency exists:

| Bazel Format | Expected Meson |
|---|---|
| `:local_target` | `local_target_cpp_lib_dep` |
| `//src/ray/foo:bar` | `bar_cpp_lib_dep` |
| `@com_google_absl//absl/...` | See AGENTS.md §10 mapping |
| `@com_github_spdlog//:spdlog` | `spdlog` |
| `@msgpack` | `msgpack_cxx` |
| `@com_google_protobuf//:protobuf` | `protobuf` |
| `@com_github_grpc_grpc//:grpc++` | `grpc` |
| `@com_github_google_flatbuffers//:flatbuffers` | `flatbuffers` |
| `@nlohmann_json` | `nlohmann_json` |
| `@com_github_redis_hiredis//:hiredis` | `hiredis` |
| `@com_github_redis_hiredis//:hiredis_ssl` | `hiredis_ssl` |
| `@com_google_googletest//:gtest_prod` | NOT listed (vendored stub) |
| `@com_google_googletest//:gtest` | NOT listed (see AGENTS.md §10 GTest) |
| Other external deps | See AGENTS.md §10 complete table |

Report:
- **Missing deps:** Bazel deps with no Meson counterpart (and no `# NOT AVAILABLE` comment)
- **Extra deps:** Meson deps not in Bazel (may indicate over-linking or transitive dep leakage)
- **NOT AVAILABLE deps:** Verify the dep truly cannot be available (is it defined in a later-processed meson.build?)

#### 2D. Dependencies — Ordering & Formatting

1. **Section comments:** Every `_dependencies` list MUST have `# Package-managed` and `# Eugo-managed` section headers with a blank line between groups (AGENTS.md Rule 20).

2. **Bazel order preserved:** Within each group, deps MUST appear in the same order as in Bazel `deps` (AGENTS.md Rule 21).

3. **NOT AVAILABLE format:** Unavailable deps MUST use the exact inline format (AGENTS.md Rule 17):
   ```
   # <meson_variable_name> is NOT AVAILABLE (processed after): //<bazel_label>
   ```
   Verify: (a) placed at correct Bazel-ordered position, (b) the Meson variable name follows naming convention, (c) the Bazel label is correct.

#### 2E. Naming Convention

| Target type | Expected variable names |
|---|---|
| Static library `foo` | `foo_cpp_lib_dependencies`, `foo_cpp_lib`, `foo_cpp_lib_dep` |
| Header-only `foo` | `foo_cpp_lib_dep` (+ optional `_dependencies`) |
| FlatBuffers `foo_fbs` | `foo_cpp_fbs` (custom_target), `foo_cpp_fbs_lib_dep` |
| C++ Proto `X` | `X_proto_cpp`, `X_proto_cpp_dep`, `X_proto_cpp_lib`, `X_proto_cpp_lib_dep` |
| C++ gRPC `X` | `X_proto_cpp_grpc`, `X_proto_cpp_grpc_dep`, `X_proto_cpp_grpc_lib`, `X_proto_cpp_grpc_lib_dep` |
| Python Proto `X` | `X_proto_py` |

Verify all variables follow this convention. Report deviations.

---

### Phase 3: Structural Checks

#### 3A. Target Ordering

`# === @begin` blocks MUST appear in the same top-to-bottom order as `ray_cc_library()` targets in BUILD.bazel (AGENTS.md §3, Rule 19).

**Allowed deviations:** When target A depends on target B and both are in the same file, B must appear before A (Meson define-before-use).

**If ANY deviation exists:** The file MUST have an `# IMPORTANT` comment block at the top explaining that ordering respects the internal dependency graph.

Report:
- Ordering mismatches without `# IMPORTANT` header
- `# IMPORTANT` header present but ordering actually matches Bazel (unnecessary header)

#### 3B. Dependency Existence

For each `*_cpp_lib_dep` referenced in a `_dependencies` list, verify it is defined in a `meson.build` that is processed BEFORE this one in the Meson build graph. The processing order is determined by the `subdir()` chain starting from `src/ray/meson.build`.

Report any references to undefined deps.

#### 3C. GTest Handling

Check if any Bazel target has `@com_google_googletest//:gtest` or `:gtest_main` in deps:
- If source only uses `FRIEND_TEST` → OK, vendored stub handles it, dep should NOT be in Meson
- If source uses `ASSERT_*`/`EXPECT_*` → should be replaced with `assert()`/`RAY_CHECK()` and annotated with `@EUGO_CHANGE`
- GTest should NEVER appear in Meson `_dependencies`

#### 3D. Platform-Specific Code

Check for Bazel `select()` expressions:
- `@platforms//os:linux` → should be `if host_machine.system() == 'linux'` in Meson
- Any other platform conditionals → verify they are handled

---

### Phase 4: Compilation Risk Assessment

Based on the above audit, flag potential compilation problems:

1. **Missing source files** → will cause undefined symbol errors at link time
2. **Missing deps** → will cause undefined symbol errors or missing headers
3. **Wrong pattern** (static_library for header-only) → duplicate symbol errors
4. **Circular deps** → build ordering failure
5. **Missing `# NOT AVAILABLE` deps** → are they truly resolved at link time via `_raylet`?
6. **Extra deps** → over-linking (not a break, but increases compile time / binary size)
7. **Missing `@EUGO_CHANGE` annotations** on any modifications to upstream files

---

### Phase 5: Proto/Codegen-Specific Checks

*Skip this phase if the file contains no codegen targets.*

1. **Proto import deps:** For each `cc_proto_library`, the corresponding `custom_target` must list all imported proto files' `*_proto_cpp_dep` in its `declare_dependency`. Check against the `.proto` file's `import` statements.

2. **gRPC depends on proto:** Every C++ gRPC target (Pattern 5) MUST depend on its corresponding C++ Proto target (Pattern 4).

3. **Python proto completeness:** Every `.proto` with a `py_proto_library` in Bazel MUST have a corresponding `custom_target` in the Python proto meson.build.

4. **Codegen kwargs:** Verify custom_targets use the correct kwargs from `eugo/utils/meson.build`:
   - `cpp_fbs_default_kwargs` for FlatBuffers
   - `proto_cpp_default_kwargs` for C++ proto
   - `proto_cpp_grpc_default_kwargs` for C++ gRPC
   - `proto_py_default_kwargs` for Python proto

---

### Output Format

Produce a structured report with these sections:

```markdown
## Review: {{MESON_PATH}}

### Summary
- Total Bazel production targets: <N>
- Total Meson targets: <N>
- Match: ✅ / ❌ (with count of missing/extra)

### Issues Found

#### CRITICAL (will cause build failure)
1. [description + fix]

#### WARNING (potential problem or convention violation)
1. [description + fix]

#### INFO (minor, cosmetic, or optimization)
1. [description]

### Target-by-Target Matrix

| Bazel Target | Meson Target | Pattern | Sources ✓ | Deps ✓ | Order ✓ | Notes |
|---|---|---|---|---|---|---|
| `target_name` | `target_name_cpp_lib` | P1/P2/... | ✅/❌ | ✅/❌ | ✅/❌ | ... |

### Deps Completeness Matrix

| Bazel Target | Bazel Dep | Meson Dep | Status |
|---|---|---|---|
| `target` | `:foo` | `foo_cpp_lib_dep` | ✅ present / ❌ missing / ⚠️ NOT AVAILABLE |
```

---

## File Inventory

Below is the complete list of meson.build files to review. Each should be instantiated with this template once.

### C++ Library Builds (src/ray/) — Review against BUILD.bazel

| # | Meson File | Corresponding BUILD.bazel |
|---|---|---|
| 1 | `src/ray/meson.build` | `src/ray/BUILD.bazel` (inline targets from multiple dirs) |
| 2 | `src/ray/common/meson.build` | `src/ray/common/BUILD.bazel` |
| 3 | `src/ray/common/cgroup2/meson.build` | `src/ray/common/cgroup2/BUILD.bazel` |
| 4 | `src/ray/common/scheduling/meson.build` | `src/ray/common/scheduling/BUILD.bazel` |
| 5 | `src/ray/core_worker/meson.build` | `src/ray/core_worker/BUILD.bazel` |
| 6 | `src/ray/core_worker/actor_management/meson.build` | `src/ray/core_worker/actor_management/BUILD.bazel` |
| 7 | `src/ray/core_worker/task_execution/meson.build` | `src/ray/core_worker/task_execution/BUILD.bazel` |
| 8 | `src/ray/core_worker/task_submission/meson.build` | `src/ray/core_worker/task_submission/BUILD.bazel` |
| 9 | `src/ray/core_worker_rpc_client/meson.build` | `src/ray/core_worker_rpc_client/BUILD.bazel` |
| 10 | `src/ray/flatbuffers/meson.build` | `src/ray/raylet/format/BUILD.bazel` (FBS schemas) |
| 11 | `src/ray/gcs/meson.build` | `src/ray/gcs/BUILD.bazel` (stub — see header comment) |
| 12 | `src/ray/gcs/actor/meson.build` | `src/ray/gcs/actor/BUILD.bazel` |
| 13 | `src/ray/gcs/postable/meson.build` | `src/ray/gcs/postable/BUILD.bazel` |
| 14 | `src/ray/gcs/store_client/meson.build` | `src/ray/gcs/store_client/BUILD.bazel` |
| 15 | `src/ray/gcs_rpc_client/meson.build` | (targets from `src/ray/rpc/BUILD.bazel` — gcs_rpc_client) |
| 16 | `src/ray/internal/meson.build` | `src/ray/internal/BUILD.bazel` |
| 17 | `src/ray/object_manager/meson.build` | `src/ray/object_manager/BUILD.bazel` |
| 18 | `src/ray/object_manager/plasma/meson.build` | `src/ray/object_manager/plasma/BUILD.bazel` |
| 19 | `src/ray/object_manager_rpc_client/meson.build` | (targets from `src/ray/rpc/BUILD.bazel`) |
| 20 | `src/ray/observability/meson.build` | `src/ray/observability/BUILD.bazel` |
| 21 | `src/ray/pubsub/meson.build` | `src/ray/pubsub/BUILD.bazel` |
| 22 | `src/ray/ray_syncer/meson.build` | `src/ray/ray_syncer/BUILD.bazel` |
| 23 | `src/ray/raylet/meson.build` | `src/ray/raylet/BUILD.bazel` |
| 24 | `src/ray/raylet/scheduling/meson.build` | `src/ray/raylet/scheduling/BUILD.bazel` |
| 25 | `src/ray/raylet_ipc_client/meson.build` | (targets from `src/ray/raylet/BUILD.bazel`) |
| 26 | `src/ray/raylet_rpc_client/meson.build` | (targets from `src/ray/rpc/BUILD.bazel`) |
| 27 | `src/ray/rpc/meson.build` | `src/ray/rpc/BUILD.bazel` |
| 28 | `src/ray/rpc/authentication/meson.build` | `src/ray/rpc/authentication/BUILD.bazel` |
| 29 | `src/ray/rpc/node_manager/meson.build` | `src/ray/rpc/node_manager/BUILD.bazel` |
| 30 | `src/ray/stats/meson.build` | (targets from `src/ray/BUILD.bazel` or inline) |
| 31 | `src/ray/thirdparty/meson.build` | `src/ray/thirdparty/BUILD.bazel` |
| 32 | `src/ray/util/meson.build` | `src/ray/util/BUILD.bazel` |
| 33 | `src/ray/util/internal/meson.build` | `src/ray/util/internal/BUILD.bazel` |

### Proto / Codegen (use Phase 5 checks)

| # | Meson File | Corresponding BUILD.bazel |
|---|---|---|
| 34 | `src/ray/protobuf/meson.build` | `src/ray/protobuf/BUILD.bazel` |
| 35 | `src/ray/protobuf/public/meson.build` | `src/ray/protobuf/public/BUILD.bazel` |
| 36 | `src/ray/protobuf/experimental/meson.build` | `src/ray/protobuf/experimental/BUILD.bazel` |
| 37 | `src/ray/protobuf/export_api/meson.build` | `src/ray/protobuf/export_api/BUILD.bazel` |

### Infrastructure (verify internal consistency, no Bazel counterpart)

| # | Meson File | Purpose |
|---|---|---|
| 38 | `meson.build` (root) | Top-level project, compiler flags, subdir ordering |
| 39 | `eugo/meson.build` | Eugo infra entry point |
| 40 | `eugo/dependencies/meson.build` | All external C++ dep declarations |
| 41 | `eugo/utils/meson.build` | Codegen kwargs (protoc, flatc wrappers) |
| 42 | `eugo/include/meson.build` | Vendored headers entry |
| 43–47 | `eugo/include/opencensus/*/meson.build` | OpenCensus proto codegen |

### Python Packaging (verify against upstream setup.py / wheel contents)

| # | Meson File | Purpose |
|---|---|---|
| 48 | `python/meson.build` | _raylet extension, install_subdir, dashboard |
| 49 | `python/ray/meson.build` | _version.py generation |
| 50 | `python/ray/dashboard/client/meson.build` | npm build |
| 51 | `src/meson.build` | C++ src entry point |

---

## Quick-Run Checklist (for each file)

Copy-paste and fill for each review:

```
File: {{MESON_PATH}}
Bazel: {{BAZEL_PATH}}

Phase 1 — Target Count:
  Bazel production targets: ___
  Meson @begin blocks: ___
  Match: ☐

Phase 2 — Per-Target:
  All patterns correct: ☐
  All sources present: ☐
  All deps complete: ☐
  Dep ordering correct: ☐
  Section comments present: ☐
  Naming convention followed: ☐

Phase 3 — Structural:
  Target ordering matches Bazel (or IMPORTANT header present): ☐
  All dep references exist: ☐
  GTest handled correctly: ☐
  Platform selects handled: ☐

Phase 4 — Compilation Risk:
  No missing sources: ☐
  No missing deps: ☐
  No wrong patterns: ☐
  No circular dep issues: ☐

Phase 5 — Codegen (if applicable):
  Proto import deps correct: ☐
  gRPC depends on proto: ☐
  Python protos complete: ☐
  Correct kwargs used: ☐

Overall: ✅ PASS / ❌ FAIL (N critical, N warning, N info)
```
