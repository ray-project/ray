# Phase 2 — Bazel + RocksDB build integration

**Status:** integration code shipped and verified. `bazel build //:rocksdb_smoke_test` succeeds; smoke test PASSES.
**Branch:** `jhasm/rep-64-poc-1`

## Claim addressed

> "RocksDB is available in [the Bazel Central Registry](https://registry.bazel.build/modules/rocksdb) (`bazel_dep(name = "rocksdb", version = "9.11.2")`) and is widely used in C++ infrastructure projects."

The REP's literal claim is BCR availability. That's true — but Ray is on legacy `WORKSPACE` (not bzlmod), so the BCR module form cannot be loaded directly. This phase therefore tests the *useful* claim underneath: that we can wire RocksDB into Ray's existing build with the same `auto_http_archive` + `bazel/BUILD.<name>` pattern used for every other C++ dep (Redis, hiredis, spdlog, jemalloc, …) without invasive patching.

## Method

Three changes, mirroring the existing dep-add convention exactly:

1. **`bazel/BUILD.rocksdb`** (new). Drives RocksDB's CMake build through `rules_foreign_cc`'s `cmake()` rule (Ray already vendors `rules_foreign_cc` 0.9.0; same rule used by `BUILD.redis`'s `make()` call). All optional features — compression (snappy / lz4 / zstd / zlib / bzip2), gflags, jemalloc, liburing, NUMA, TBB, RocksDB shell tools, RocksDB tests, examples, benchmarks — are explicitly `OFF`. `PORTABLE=ON` so the artifact is portable across x86_64 microarchitectures. Output is a single static library `librocksdb.a`.
2. **`bazel/ray_deps_setup.bzl`** — adds an `auto_http_archive(name = "com_github_facebook_rocksdb", ...)` entry pointing at the GitHub release tarball for RocksDB 9.11.2 (the version named in the REP). SHA256 verified against the upstream tarball: `0466a3c220464410687c45930f3fa944052229c894274fddb7d821397f2b8fba`.
3. **`BUILD.bazel`** — adds a `ray_cc_test(name = "rocksdb_smoke_test", ...)` that depends only on `@com_github_facebook_rocksdb//:rocksdb` and gtest. Tagged `team:core` to match every other StoreClient test target. Constrained to `@platforms//os:linux` (Phase 8 will add cross-platform coverage if needed).
4. **`src/ray/gcs/store_client/test/rocksdb_smoke_test.cc`** (new). The smallest possible exercise: open a RocksDB at a unique temp dir, `Put` one key under `WriteOptions::sync = true`, `Get` it back, assert the value, clean up. Roughly 50 lines including license. Not a `StoreClient` implementation — that arrives in Phase 3.

## Result

### What we verified on this run

- **`bazel info` loads against Ray's `WORKSPACE`** — confirms our edits to `ray_deps_setup.bzl` are at least parse-clean. Ran with Bazel 9.0.0, `JAVA_HOME=/export/apps/jdk/JDK-11_0_10_9-msft`.
- **RocksDB tarball integrity.** Independently downloaded `v9.11.2.tar.gz` from `github.com/facebook/rocksdb` and computed `sha256(0466…8fba)`. Matches the value pinned in `ray_deps_setup.bzl`. Tarball ships `LICENSE.Apache` (Apache 2.0) and `LICENSE.leveldb` (BSD 3-clause); both are compatible with Ray's Apache 2.0.
- **C++ source compiles against the real RocksDB headers.** GCC 11.2 `--std=c++17 -fsyntax-only` over an isolated test (the `Open`/`Put`/`Get` portion of `rocksdb_smoke_test.cc`) against unpacked `rocksdb-9.11.2/include/`: 0 errors, 0 warnings.

### What we did NOT verify on this run

This dev VM is missing tooling needed to drive the full integration end-to-end:

| Tool | Available? | Why it matters |
|---|---|---|
| Upstream Bazel 5.4.1+ (matching Ray's WORKSPACE) | No — only LinkedIn-internal Bazel 9.0.0 wrapper. Bazel 9 strict mode rejects Ray's WORKSPACE-style external repos at query time (`unknown repo 'python3_9'`). | A real `bazel build //:rocksdb_smoke_test` needs an older Bazel that fully supports Ray's WORKSPACE protocol. |
| `cmake` | No (`/bin/bash: cmake: command not found`). | `rules_foreign_cc`'s `cmake()` rule shells out to host `cmake`. |
| `ninja` | No. | Same — `generate_args = ["-G Ninja"]` in `BUILD.rocksdb`. |
| Reachable network from the Bazel sandbox | Unclear (LinkedIn truststore warnings on the bazel wrapper itself). | First build needs to fetch the tarball. |

So the explicit acceptance criteria from the PLAN — "Builds clean with `--config=ci` and `--config=asan-clang`" + "binary delta numbers" + "license report" — are not yet checked. They are not blocked on the design; they are blocked on running the integration on a machine that has Bazel + CMake + Ninja + internet (a clean Linux laptop, a kind/CI runner, or a cloud VM).

### Provisional answer to the underlying questions

| REP claim | Phase 2 evidence so far |
|---|---|
| The dep can be wired into Ray's build via the same idiom as every other C++ dep. | **Provisional yes.** Files written using the existing pattern; SHA verified; trivial source compiles against real headers. Not yet built. |
| The integration won't require invasive third-party patching. | **Provisional yes.** No patches added (`patches = []`). RocksDB ships clean upstream CMake. |
| Binary size growth is bounded. | **Unknown.** Needs a real build on a properly tooled host. Tracked in **R3**. |
| Builds clean under ASAN. | **Unknown.** Same. |

## Skepticism

### What this provisional pass does *not* prove

- **The CMake invocation actually produces `librocksdb.a` Ray's targets can link against.** `rules_foreign_cc`'s `cmake()` has its own conventions for header propagation, and the `out_static_libs = ["librocksdb.a"]` line is the contract — if the actual artifact is named `librocksdb-shared.a`, or installed under a different prefix, the smoke test fails to link. We won't know until a real build runs.
- **`PORTABLE=ON` actually portable.** RocksDB's `PORTABLE` flag means "no `-march=native`," which keeps the build artifact runnable on older microarchitectures, but it doesn't suppress all the SSE4.2 / AVX2 detection in `port/` — if Ray runs on an environment without those, we'd need additional flags.
- **No transitive dep collisions with Ray's existing graph.** Ray already vendors snappy- / zstd-adjacent things indirectly via Arrow / Parquet (in the Ray Data path). Our RocksDB build has compression `OFF` so this should not bite, but it has not been confirmed.
- **`ASAN-clang` cleanliness.** RocksDB has occasional ASAN findings in pre-release builds. We will not know if 9.11.2 is clean against Ray's exact `--config=asan-clang` until we build.

### What would invalidate the result

- Building on a real host produces a transitive-dep graph that pulls in something Ray cannot ship (an LGPL / GPL transitive). Mitigation: `BUILD.rocksdb` keeps everything `OFF`, but a real build's `bazel query` of the rocksdb deps is the only ground truth.
- Binary size delta on the head ray binary > 50 MB after stripping. Trips **R3**'s pivot trigger.

### What R-register status changes

- **R1 (Bazel integration).** Reduced from "open" to **"likely yes, pending real build."** SHA verified, pattern matches existing deps, C++ source compiles. Final close-out requires a real build.
- **R2 (toolchain).** Reduced from "open" to **"likely yes, pending real build."** GCC 11 cleanly parsed RocksDB headers in C++17 mode, and Ray's CI runs newer GCC than that.

## Reproducer

On a host with Bazel ≥ 5.4.1 (matching Ray's WORKSPACE), CMake, Ninja, and outbound HTTPS:

```bash
# Smoke test build:
bazel build --config=ci //:rocksdb_smoke_test

# ASAN cleanliness:
bazel build --config=ci --config=asan-clang //:rocksdb_smoke_test

# Run it:
bazel test --config=ci //:rocksdb_smoke_test --test_output=streamed
```

Independently verifiable:

```bash
# Verify the upstream tarball hash matches what we pinned.
curl -fsSL -o /tmp/rocksdb.tar.gz \
  https://github.com/facebook/rocksdb/archive/refs/tags/v9.11.2.tar.gz
sha256sum /tmp/rocksdb.tar.gz
# Expected: 0466a3c220464410687c45930f3fa944052229c894274fddb7d821397f2b8fba
```

## Pivot decision

**Proceed.** The integration code is written using the same idiom Ray already uses for every C++ dep, the SHA matches upstream, and the trivial RocksDB API call compiles against the real headers. No reason to pivot to SQLite (the REP's runner-up) on what we've seen. The PLAN's success criteria for Phase 2 — "builds clean under `--config=ci` and `--config=asan-clang`" + binary delta + license report — are tracked as follow-on work to be closed on a properly tooled host before Phase 3's walking skeleton lands.

## Next concrete actions before closing Phase 2

1. Run `bazel build //:rocksdb_smoke_test` on a clean Linux host with Bazel 5/6/7 + CMake + Ninja. Capture: stdout (transitive dep graph), wall-clock time, final binary size, ASAN status.
2. From the build output, enumerate transitive deps and confirm licenses (snappy / lz4 / zstd are off, but anything `liburing`-adjacent or kernel-layer needs to be checked).
3. Append a "Built on" subsection to this report with the captured numbers.
4. Pin RocksDB to a stricter version once 9.11.2 is confirmed working — e.g., bump to whatever's current on the PR-merge date and re-verify, so the POC tracks an actively maintained release.

## Built on (2026-04-30)

Re-ran on the same dev VM after installing `~/.local/bin/{bazel,cmake,ninja}` and bypassing the LinkedIn `/usr/local/linkedin/bin/bazel` wrapper via `PATH` ordering. Bazelisk auto-resolved Bazel **5.4.1** for Ray's WORKSPACE.

**Findings during this run:**

1. **`PORTABLE=ON` was already correct, but `out_static_libs = ["librocksdb.a"]` was wrong.** First two build attempts failed at the linker step with `output 'external/com_github_facebook_rocksdb/rocksdb/lib/librocksdb.a' was not created`. The sandbox child exited with code 0 (cmake/ninja finished cleanly) but the path was wrong — RocksDB's CMake `GNUInstallDirs` picks `lib64/` on x86_64 Linux. Fix: added `"CMAKE_INSTALL_LIBDIR": "lib"` to `cache_entries` so the install path is portable across Linux distros (some use `lib`, glibc-on-x86_64 uses `lib64`) and macOS / BSD (always `lib`).
2. **Boost dep was broken upstream.** Ray's WORKSPACE pinned `https://boostorg.jfrog.io/artifactory/main/release/1.81.0/source/boost_1_81_0.tar.bz2`, which now serves a different archive (SHA `1c1...`) than Ray expects (`71f...`). The bazel-mirror fallback URLs return 404. Switched to `https://archives.boost.io/release/1.81.0/source/boost_1_81_0.tar.bz2` — canonical Boost mirror, matches the pinned SHA exactly. Verified manually with `curl + sha256sum`. *(This fix is not REP-64-specific; it would block any current `master` Ray build.)*

**Build measurements (cold cache, after fixes):**

| Step | Wall-clock |
|---|---|
| Bazel analysis | 1 s |
| RocksDB cmake build (foreign_cc) | ~225 s (3:45) |
| Link + finalize `rocksdb_smoke_test` | ~3 s |
| **Total cold build** | **229 s** |
| Smoke test execution (`bazel test //:rocksdb_smoke_test`) | **64 ms** |

`bazel-bin/rocksdb_smoke_test` stat'd at first build:
- Size: **8.3 MB** unstripped (`8652544` bytes), ELF64 PIE, dynamically linked. Most of this is RocksDB itself + gtest + libstdc++/libc symbols. Stripped delta will be smaller; not yet measured.

**Test result:**
```
[==========] Running 1 test from 1 test suite.
[ RUN      ] RocksDbSmoke.OpenPutGet
[       OK ] RocksDbSmoke.OpenPutGet (64 ms)
[  PASSED  ] 1 test.
```

**Open items after this run:**
- ASAN cleanliness (`--config=asan-clang`) — not yet attempted on this host. Requires the LLVM toolchain Ray expects, which is not in `~/.local/`.
- Binary size delta on `gcs_server` — Phase 3 territory. Captured separately once `rocksdb_store_client_test` builds.
- Transitive dep license enumeration — `bazel query 'deps(@com_github_facebook_rocksdb//:rocksdb)'` not yet captured.

**R-register update:**
- **R1 (Bazel integration).** Closed: **"yes."** Real build green, smoke test passes against the real RocksDB.
- **R2 (toolchain).** Closed: **"yes for GCC 11 / Bazel 5.4.1."** Pending re-confirm on `--config=asan-clang`.
