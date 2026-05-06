# macOS arm64 dev setup — local changes for build & test

This branch was developed on Linux. Bringing the same workflow up on macOS
arm64 (Apple Silicon, Bazel 7.5.0) required a small set of changes — some
are general portability fixes, a couple are POC-scoped. This doc lists every
change, why it's needed, and where to look in the diff.

**Code changes are not committed.** Ray's supported build target is Linux,
so we don't want to land macOS-only quirks in the tree. Instead, the changes
ship as a patch file (`rep-64-poc/macos-dev.patch`) that another macOS
contributor can apply locally to reproduce this setup:

```bash
git apply rep-64-poc/macos-dev.patch    # apply on top of a clean checkout
# ... do mac dev work ...
git apply -R rep-64-poc/macos-dev.patch # revert before pushing
```

The patch and this doc are the only files committed for the macOS workflow.

**Verified green on macOS arm64:**

```bash
~/.local/bin/bazel-7.5.0 build --config=ci --copt=-Wno-deprecated-builtins //src/ray/...
~/.local/bin/bazel-7.5.0 build --config=ci --copt=-Wno-deprecated-builtins //:all //cpp:all
source .venv/bin/activate && pytest python/ray/tests/test_basic.py
```

## Code changes (in `macos-dev.patch`)

Each change below corresponds to one hunk in `rep-64-poc/macos-dev.patch`.

### 1. `.bazelrc` — force static linking globally

```
build --compilation_mode=opt
+# Force static linking to avoid cross-package upb dylib symbol resolution
+# failures (envoy_api/googleapis/grpc) seen on macOS arm64 with Bazel 7.
+build --dynamic_mode=off
```

**Why:** with the default `--dynamic_mode=default`, gRPC's `upb_proto_library`
rule generates per-`proto_library` `.dylib` shared libs (envoy_api,
googleapis, grpc) on macOS. Cross-package symbol references (`*_msg_init`,
`*_proto_upbdefinit`) aren't propagated to each dylib's link line, so linking
fails with `Undefined symbols for architecture arm64`. Forcing static
linking dodges the issue. Doesn't affect Linux behavior in any meaningful
way (static linking is the conservative choice anyway).

### 2. `BUILD.bazel` — gate `:jemalloc_files` to Linux

```python
 pkg_files(
     name = "jemalloc_files",
     srcs = ["@jemalloc//:shared"],
     attributes = pkg_attributes(mode = "755"),
     prefix = "ray/core/",
+    target_compatible_with = ["@platforms//os:linux"],
     visibility = ["//visibility:private"],
 )
```

**Why:** the only consumer of `:jemalloc_files` (`:ray_pkg_zip` at
`BUILD.bazel:362`) already routes it through `select({":jemalloc": [...]})`,
where `:jemalloc` requires `@platforms//os:linux`. But `:jemalloc_files`
itself had no platform constraint, so `bazel build //:all` on macOS
expanded it as a top-level target → forced jemalloc's `make -j` to run →
that crashed with `Segmentation fault: 11` inside `rules_foreign_cc`'s
hermetic `make`. Adding `target_compatible_with` makes the wildcard skip
the target, matching the existing consumer constraint.

### 3. `src/ray/common/cgroup2/BUILD.bazel` — gate `:sysfs_cgroup_driver` to Linux

```python
 ray_cc_library(
     name = "sysfs_cgroup_driver",
     srcs = ["sysfs_cgroup_driver.cc"],
     hdrs = ["sysfs_cgroup_driver.h"],
+    target_compatible_with = [
+        "@platforms//os:linux",
+    ],
     visibility = [":__subpackages__"],
     ...
 )
```

**Why:** `sysfs_cgroup_driver.h` includes `<mntent.h>`, which doesn't exist
on macOS. The only consumer (`:cgroup_manager_factory`) already gates it
via `select({"//bazel:is_linux": [...]})`, and sibling targets in the same
file (`:fake_cgroup_driver`, `:cgroup_test_utils`) already have the
constraint — `:sysfs_cgroup_driver` was the lone outlier. This is a
pre-existing portability bug, unrelated to REP-64.

### 4. `src/ray/gcs/store_client/BUILD.bazel` — drop Linux-only constraint on `:rocksdb_store_client` *(POC-scoped)*

```python
 ray_cc_library(
     name = "rocksdb_store_client",
     srcs = ["rocksdb_store_client.cc"],
     hdrs = ["rocksdb_store_client.h"],
-    target_compatible_with = ["@platforms//os:linux"],
     deps = [
         ":store_client",
         ...
         "@com_github_facebook_rocksdb//:rocksdb",
         ...
     ],
 )
```

**Why:** the constraint was added in the original POC commit
(`f19038586c`) when development happened on Linux. The implementation
itself is portable — only standard C++ + RocksDB + abseil + boost::asio.
RocksDB's CMake build under `bazel/BUILD.rocksdb` already explicitly
handles macOS (`CMAKE_INSTALL_LIBDIR = "lib"` with a comment about
macOS/BSD lib layout). On macOS arm64, `librocksdb.a` builds in ~100s and
links cleanly into `:rocksdb_store_client`.

The two test targets in `tests/BUILD.bazel` (`rocksdb_smoke_test`,
`rocksdb_store_client_test`) keep their Linux-only constraints —
relaxing those would need a separate validation pass.

### 5. `src/ray/gcs/store_client/rocksdb_store_client.h` — `[[maybe_unused]]` on `io_service_` *(POC-scoped)*

```cpp
   // Holds a ref so Postable's default-IO-context resolution still works,
   // matching how RedisStoreClient stores its io_service_.
-  instrumented_io_context &io_service_;
+  [[maybe_unused]] instrumented_io_context &io_service_;
```

**Why:** clang on macOS warns about unused private fields
(`-Wunused-private-field`), and Ray builds with `-Werror`. The reference
is intentionally kept for Postable's default-IO-context resolution but
never re-read inside the class — exactly the pattern this warning is
designed to flag. gcc on Linux doesn't have this diagnostic, so it
silently passed there. `[[maybe_unused]]` is the portable C++17
acknowledgement.

### 6. `src/ray/raylet/node_manager.cc` — honor `agent_register_timeout_ms` for port-file waits

```cpp
 std::tuple<int, int, int> NodeManager::WaitForDashboardAgentPorts(
     const NodeID &self_node_id, const NodeManagerConfig &config) {
+  // Use the same wait budget as runtime_env_agent_client so a slow first-time
+  // import in the dashboard agent doesn't crash the raylet on platforms where
+  // 15s isn't enough (e.g. macOS dev machines). Default is 30s on POSIX, 100s
+  // on Windows; overridable via RAY_agent_register_timeout_ms.
+  const int timeout_ms =
+      static_cast<int>(RayConfig::instance().agent_register_timeout_ms());
   int metrics_agent_port = config.metrics_agent_port;
   if (metrics_agent_port == 0) {
     RAY_ASSIGN_OR_CHECK_SET(
         metrics_agent_port,
-        WaitForPersistedPort(config.session_dir, self_node_id, kMetricsAgentPortName));
+        WaitForPersistedPort(
+            config.session_dir, self_node_id, kMetricsAgentPortName, timeout_ms));
   }
   ...
 }

 int NodeManager::WaitForRuntimeEnvAgentPort(...) {
   ...
   RAY_ASSIGN_OR_CHECK_SET(
       int port,
-      WaitForPersistedPort(config.session_dir, self_node_id, kRuntimeEnvAgentPortName));
+      WaitForPersistedPort(config.session_dir,
+                           self_node_id,
+                           kRuntimeEnvAgentPortName,
+                           static_cast<int>(
+                               RayConfig::instance().agent_register_timeout_ms())));
   return port;
 }
```

**Why:** `WaitForPersistedPort` defaults to 15s
(`src/ray/util/port_persistence.h:79`). On a cold-start macOS dev
machine, the dashboard agent's `ReporterAgent.__init__` takes ~35s
(opentelemetry + metrics setup), so the raylet panics with
`Check failed: ... Timed out waiting for file metrics_export_port_*`
and crashes node startup. Every test that calls `ray.init()` then fails.

`agent_register_timeout_ms` already exists in `ray_config_def.h` (30s
POSIX / 100s Windows, env-overridable as `RAY_agent_register_timeout_ms`)
and is used by `runtime_env_agent_client`. Threading it through
`WaitForDashboardAgentPorts` and `WaitForRuntimeEnvAgentPort` unifies
the two agents under one knob. CI/Linux servers start fast and don't
notice the change; slow machines can bump the env var without recompiling.

## Local environment setup (not committed)

These are dev-machine setup steps, not code changes. They're the standard
flow described in `doc/source/ray-contribute/development.rst` plus a few
extras for `bazel test` / `pytest` on macOS.

```bash
# 1) Python venv (Python 3.13)
python3 -m venv .venv
source .venv/bin/activate
pip install -r python/requirements.txt
pip install pytest psutil proxy.py        # pytest + test fixture deps

# 2) Editable Ray install — triggers a full Bazel build via setup.py
cd python
BAZEL_PATH=$HOME/.local/bin/bazel-7.5.0 \
BAZEL_ARGS="--copt=-Wno-deprecated-builtins" \
    pip install -e . --verbose
cd ..

# 3) Dashboard frontend (otherwise ray.init() raises FrontendNotFoundError)
cd python/ray/dashboard/client
npm ci
npm run build
cd -
```

Notes:

- `BAZEL_PATH` is needed because `setup.py:_find_bazel_bin` looks for
  `bazelisk`/`bazel`/`~/bin/bazel` and doesn't find `~/.local/bin/bazel-7.5.0`.
- `BAZEL_ARGS="--copt=-Wno-deprecated-builtins"` propagates the
  abseil/clang `__is_trivially_relocatable` workaround into the
  setup.py-driven build (the bare `bazel test` invocation has it on
  the CLI; setup.py doesn't add it automatically).
- `proxy.py` provides the `proxy` CLI that `start_http_proxy` fixture
  in `python/ray/tests/conftest.py:1421` shells out to.
- `npm run build` produces `python/ray/dashboard/client/build/`, which
  the dashboard's `setup_static_dir()` requires. Without it,
  `ray.init()` fails with `FrontendNotFoundError`.

## How to verify

After applying the patch (`git apply rep-64-poc/macos-dev.patch`) and
finishing the local setup:

```bash
# 1) Bazel C++ build — all of //src/ray/...
~/.local/bin/bazel-7.5.0 build --config=ci --copt=-Wno-deprecated-builtins //src/ray/...

# 2) Bazel package targets used by setup.py
~/.local/bin/bazel-7.5.0 build --config=ci --copt=-Wno-deprecated-builtins //:all //cpp:all

# 3) Python tests via plain pytest (Ray's documented dev workflow;
#    `bazel test` for Python isn't intended for local use — see
#    .bazelrc:171-176).
source .venv/bin/activate
pytest python/ray/tests/test_basic.py
```

`test_omp_threads_set` is a useful first probe — it exercises the full
`ray.init()` → raylet → dashboard agent path that the timeout fix
addresses.

## Change scope summary

| File | Scope |
|---|---|
| `.bazelrc` | macOS portability (no Linux behavior change) |
| `BUILD.bazel` | macOS portability — pre-existing `//:all` wildcard bug |
| `src/ray/common/cgroup2/BUILD.bazel` | macOS portability — pre-existing constraint outlier |
| `src/ray/gcs/store_client/BUILD.bazel` | POC — relax Linux-only on the new RocksDB StoreClient |
| `src/ray/gcs/store_client/rocksdb_store_client.h` | POC — clang-on-macOS warning fix |
| `src/ray/raylet/node_manager.cc` | macOS portability + Linux benefit (unifies agent timeout knobs) |

The first three and the last one are general fixes — they'd benefit any
macOS contributor independent of REP-64. The two POC-scoped entries are
needed only because this branch ships the new RocksDB StoreClient.
