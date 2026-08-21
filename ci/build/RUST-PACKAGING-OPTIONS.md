# Packaging Rust extensions in Ray

Working notes on how a PyO3/Rust extension can ship with Ray, written while
reviewing [#65117](https://github.com/ray-project/ray/pull/65117) (a flag-gated
arrow-rs Parquet reader for Ray Data). Ray does not package any Rust today, so
this records what the build system actually permits, what it does not, and why.

## Where Ray's build stands today

| Fact | Where to verify |
| --- | --- |
| `python/` has **no `pyproject.toml`**; the build is legacy `setup.py`/setuptools | `ls python/` |
| Root `pyproject.toml` has **no `[build-system]` table** — only `[tool.ruff]`, `[tool.mypy]`, `requires-python` | `pyproject.toml` |
| `setup.py` **compiles nothing**. Native artifacts come from Bazel and are `shutil.copy`'d into `build_lib` | `pip_run()` / `copy_file()` in `python/setup.py` |
| `has_ext_modules()` returns `True` purely to force a platform wheel | `python/setup.py` |
| **No Rust toolchain anywhere** — `build-manylinux-forge.sh` installs bazelisk, node 14, JDK 8 only | zero hits for `cargo|rustup|maturin|rustc` under `ci/`, `docker/` |
| Wheel matrix: linux x86_64 + aarch64, macOS x86_64 + arm64, Windows × py3.10–3.14 | `python/build-wheel-*.sh` |
| Precedent for a second wheel from this repo: `ray-cpp` | `RAY_INSTALL_CPP=1`, `RayCppBdistWheel` in `python/setup.py` |
| Precedent for a **separately-published native wheel consumed as a dependency**: `ray-haproxy` | `python/requirements.txt`; `setup_spec.extras["serve"]` |

`ray-haproxy` is the important one. It is built in
[ray-project/ray-haproxy](https://github.com/ray-project/ray-haproxy) by a GitHub
Actions matrix, inside the same manylinux2014 container tag Ray pins, published
to PyPI via trusted publishing as `py3-none-manylinux_2_17_{x86_64,aarch64}`,
then consumed here as one line:

```
ray-haproxy>=2.8.25,<2.9.0; sys_platform == 'linux'
```

which flows through raydepsets into every `requirements_compiled*.txt` and
`deplocks/ci/*.lock`. The plumbing to consume an out-of-tree native wheel is
already built and in production.

## `rules_rust` is not available to Ray

Ray's C++ comes from Bazel, so building the crate as another Bazel target and
letting `pip_run()` copy the `.so` — exactly how `_raylet.so` is handled — looks
like the natural fit. It is not currently possible:

- Ray is on Bazel **7.5.0** in **WORKSPACE mode**: `.bazelrc` line 1 is
  `common --noenable_bzlmod`, and there is no `MODULE.bazel`.
- `rules_rust` **removed WORKSPACE support entirely** in
  [PR #4005](https://github.com/bazelbuild/rules_rust/pull/4005) (merged
  2026-05-01, shipped in **0.71.0**). Current is 0.73.0.
- Releases 0.71.0–0.72.0 shipped `http_archive` snippets that do not work;
  [PR #4180](https://github.com/bazelbuild/rules_rust/pull/4180) removed them
  from the template.
- The last WORKSPACE-capable release is **0.70.0** (2026-04-22) — a stale, dead
  branch.
- **`rules_rust_pyo3` — the piece actually needed — has never had a WORKSPACE
  path in any release.** It is a Bazel module by construction:
  `module(name = "rules_rust_pyo3")` with `bazel_dep(rules_python)` and a bzlmod
  `use_extension` for its pyo3 crates. At 0.70.0 it pins pyo3 0.28.2.

Bazel 7 does support bzlmod (it is default-on; Ray opted out) and supports hybrid
`MODULE.bazel` + `WORKSPACE` mode, so a narrow path exists: enable bzlmod, add a
minimal `MODULE.bazel` for `rules_rust` + `rules_rust_pyo3`, keep everything else
in WORKSPACE. But flipping bzlmod changes how existing deps resolve (`rules_cc`,
protobuf, `bazel_skylib`, `rules_python`), which is the classic way to break a
large C++ build. Bazel 9 removes WORKSPACE, so Ray must migrate eventually —
just not on one reader's schedule.

**Conclusion: blocked on bzlmod migration. Revisit after.**

## Why not build Rust inside the `ray` wheel

Beyond `rules_rust`, in-tree compilation via `setuptools-rust` (the only Rust
integration that works with a legacy `setup.py`) requires:

1. Adding a `[build-system]` table, or pre-installing `setuptools-rust` in every
   build environment, since there is no isolated-build metadata to declare it in.
2. Adding `rustc` to the manylinux forge, the macOS build hosts, and Windows.
3. Absorbing a multi-minute cold `cargo` build per platform — `parquet` +
   `arrow` + `object_store` at `opt-level=3, lto=true, codegen-units=1`. **Bazel's
   remote cache does not cover cargo.**
4. A crates.io story compatible with the CodeArtifact/pip-mirror work: cargo is a
   third package ecosystem fetching at build time, needing `CARGO_NET_*`,
   `source.crates-io.replace-with`, or a vendored `vendor/` tree.
5. License attribution for the crate graph (the PR's `Cargo.lock` is 2,637 lines)
   and CVE scanning that understands crates. `ray-haproxy` carries a
   `THIRD_PARTY_LICENSES` file and a `grype --fail-on high` gate for this reason.

It also forces the binary on every Ray user, and a single wheel cannot be
linux-only. For an **optional, flag-gated** reader that is the wrong trade.

Note that `pyca/cryptography`, long the flagship `setuptools-rust` user, has
since migrated to `build-backend = "maturin"`.

## What the ecosystem does

| Project | Layout | Backend | How Python gets the Rust |
| --- | --- | --- | --- |
| **polars** | `py-polars/` in monorepo | **`setuptools.build_meta`** | `dependencies = ["polars-runtime-32 == 1.43.2"]`; `rt64`/`rtcompat` extras select alternative runtime wheels |
| **pydantic** | separate repo | maturin | `pydantic-core==2.48.0` exact pin |
| **delta-rs** | `python/` in monorepo | maturin | one wheel; `pyo3-arrow` for the Arrow boundary |
| **datafusion-python** | separate repo | maturin | one wheel; pyo3 0.29, arrow 59, object_store 0.13.1 |
| **tokenizers** | `bindings/python/` | maturin | one wheel |
| **cryptography** | `src/rust/` | maturin (was setuptools-rust) | one wheel |

**polars is the direct precedent for Ray's situation:** the Python-facing package
is built by plain setuptools with no Rust toolchain involved, and the compiled
extension is a *separately published, exact-pinned wheel*. Selecting between
runtime builds via extras is exactly the shape an optional native reader wants.

Nobody in this list buries the crate inside the Python package tree — they all
use a sibling `python/`, `py-*/`, or `bindings/` directory. #65117 currently
places it at `python/ray/data/_internal/datasource_v2/native/ray_data_arrow_rs/`,
**with its own `pyproject.toml`**, which is a hazard for `pip install -e python/`,
sdist builds, and any tool that walks up to the nearest `pyproject.toml`.

## Recommended sequence

1. **Merge the Python side flag-off.** The reader, PyArrow fallback, metrics and
   `DataContext` flag can land as inert code — the import is lazy inside
   `_arrow_rs_supported`. Move the crate to a repo-root `rust/`.
2. **Build the wheel once in CI; install it where it is needed.** Implemented in
   this change: `ci/build/build-arrow-rs-wheel.sh` plus a layered
   `dataarrowrsbuild` image. crates.io is touched by one cached image build
   rather than by every image, and the artifact produced is the same one a PyPI
   package would ship.
3. **Split into `ray-project/ray-data-arrow-rs`** on the `ray-haproxy` pattern:
   tag-driven GH Actions matrix over manylinux2014 x86_64/aarch64, abi3 wheels,
   `cargo-audit`/grype gate, `THIRD_PARTY_LICENSES` via `cargo-about`, PyPI
   trusted publishing. Ray side becomes one line in `extras["data"]`.
4. **Revisit `rules_rust`** only if the reader becomes non-optional, and only
   after the bzlmod migration.

## Test-coverage gaps in #65117 as it stands

Both are packaging-independent and worth fixing regardless of the path chosen:

- `python/ray/data/tests/datasource/test_arrow_rs_parquet_reader.py` (2,643 lines)
  opens with `pytest.importorskip("ray_data_arrow_rs")`, so it **skips silently**
  wherever the extension is absent — which is everywhere in premerge.
- The PR adds **no `BUILD.bazel` target** for that file, so
  `//python/ray/data/...` never enumerates it. The tests cannot run even if the
  extension were installed.

This change adds the `py_test` target (tagged `arrow_rs`), excludes that tag from
the general data jobs so they cannot silently skip it, and adds a dedicated job
that runs it against an image where the extension is actually present.

## Notes for the crate itself

- **pyo3 0.22 → 0.29.** 0.22 is seven releases behind; free-threaded support
  landed in 0.23. `datafusion-python` — the closest architectural twin — is on
  0.29 with the same arrow 59 / object_store 0.13.1 pins, so only pyo3 is
  off-trend. abi3 wheels do not load on free-threaded builds, so if Ray ever
  ships `cp314t` a separate non-abi3 build is required. (Ray does not build
  `cp314t` today.)
- **`abi3-py39` is doing real work:** one binary per platform serves every
  CPython ≥3.9, making the matrix 5 wheels rather than 25. `build-arrow-rs-wheel.sh`
  asserts the wheel tag stays `abi3` so this cannot silently regress.
- **The Arrow C Stream / PyCapsule boundary is the right call** — with only
  `arrow`'s `ffi` feature there is no pyarrow ABI coupling, so one build serves
  all pyarrow versions. This is what makes out-of-tree packaging clean. Consider
  the `pyo3-arrow` crate (used by delta-rs) instead of hand-rolling it.
- **Pin the API before the first release.** The crate's README lists
  `decode_budget_bytes`, `k`, `fetch_window_mb` as unexposed; those are API
  surface, and a `>=0.1,<0.2` bound is only honest once the signature settles.
