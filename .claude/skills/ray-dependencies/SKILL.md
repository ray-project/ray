---
name: ray-dependencies
description: Manage Python dependencies in Ray — add/remove/upgrade packages, work with raydepsets lock files, debug dependency conflicts, and regenerate compiled requirements. Covers `python/requirements*`, `python/requirements/**`, `python/deplocks/**`, and `ci/raydepsets/configs/*.depsets.yaml`.
user-invocable: true
argument-hint: <question or task>
---

# Ray Dependencies

Expert skill for managing Python dependencies across the Ray repository: the monorepo `requirements_compiled*.txt` lock files, the raydepsets DAG-based lock file manager, modular `python/requirements/**` source files, and Docker image dependency chains.

---

## When to use this skill

**Use this skill when the user wants to:**

- Add, remove, or upgrade a Python dependency in Ray
- Create or modify a `.depsets.yaml` config file
- Understand how dependencies flow between Ray Docker images
- Debug dependency conflicts or version resolution failures
- Regenerate lock files after requirement changes
- Create a new CI test environment with locked dependencies
- Understand why a specific package version was chosen

**Not for:**

- General Python packaging questions unrelated to Ray
- Ray runtime/API questions (use Ray docs)
- Docker image build issues unrelated to dependencies

---

## Workflow rules (project preferences)

These take precedence over generic packaging advice:

1. **Source-file pins only.** Resolve conflicts by editing `python/requirements.txt`, `python/requirements/**/*.txt`, or `python/requirements/**/*.in`. Do **not** hand-edit `python/requirements_compiled*.txt` — it is a build artifact and gets overwritten on the next compile.
2. **No `ci/ci.sh` post-process hooks** for individual package fixes. Only touch `ci/ci.sh` for genuinely structural changes (new functions, new input files passed to the compiler). Never as an after-the-fact lock mutator.
3. **Use the dual-exact-pin-with-markers pattern** when a package needs different versions on different Python versions and the lock is consumed as a constraint across multiple Python targets. See *Marker preservation* below.
4. **`python/requirements.txt` ↔ `python/setup.py` must stay in sync.** The packages under the `## setup.py install_requires` block in `python/requirements.txt` are mirrored into `setup_spec.install_requires` in `python/setup.py` (around line 405). When you add, remove, or change a version bound for any package in that block, edit **both** files in the same change. The list in `setup.py` is what end users actually install via `pip install ray`; `requirements.txt` is the dev-side source of truth. Drift between them ships broken installs.
5. **`python/requirements/llm/llm-requirements.txt` ↔ `setup_spec.extras["llm"]` must stay in sync.** The `ray[llm]` extra in `python/setup.py` (around line 377) and `python/requirements/llm/llm-requirements.txt` are paired source-of-truth files for the LLM install set — both have explicit "keep in sync" comments. When you add, remove, or change a permanent version bound for any package in the LLM extra, edit **both** files. **Exception:** temporary upper-bound workaround pins (`<=X.Y.Z` to dodge a bug) may live only in `llm-requirements.txt`. They must NOT be added to `setup.py`, since `setup.py` is the public install constraint and should not advertise short-lived workarounds as future API. Strip the `<=` pin from both files once the upstream fix lands.
6. **When presenting options, list source-file fixes first.** Mention lock or `ci.sh` edits only as fallbacks with downsides called out.

---

## Ray Dependency Architecture

Ray uses a two-tier dependency management system:

1. **`requirements_compiled*.txt`** — monorepo-wide pinned dependency files, compiled via `ci/ci.sh compile_pip_dependencies`. One per Python version: `requirements_compiled.txt` (default), `requirements_compiled_py3.10.txt`, `requirements_compiled_py3.11.txt`, etc.
2. **raydepsets** — a DAG-based lock file manager (`ci/raydepsets/`) that generates per-image, per-environment lock files from `.depsets.yaml` configs. Lock files live in `python/deplocks/` and `release/ray_release/byod/`.

### Key file locations

| Path | Purpose |
|---|---|
| `python/requirements.txt` | Base Ray installation requirements |
| `python/requirements_compiled.txt` | Monorepo pinned dependencies (per-Python-version variants exist) |
| `python/requirements/` | Modular requirement files by component (test, ml, data, train, tune, serve, rllib, llm) |
| `python/requirements/ml/py313/` | ML requirements organized by Python version |
| `python/requirements/data/` | Ray Data variant requirements (pyarrow versions, mongo, etc.) |
| `python/requirements/llm/` | LLM requirements (`llm-requirements.txt`, `llm-test-requirements.txt`) |
| `python/requirements/serve/` | Serve requirements and overrides |
| `python/deplocks/` | Generated lock files (output of raydepsets) |
| `docker/base-deps/requirements.in` | Docker base-deps layer requirements |
| `docker/base-extra/requirements.in` | Docker base-extra layer requirements |
| `docker/base-slim/requirements.in` | Docker slim image requirements |
| `release/ray_release/byod/` | BYOD (Bring Your Own Dependencies) files for release tests |

---

## Generating the constraints (`requirements_compiled*.txt`)

The two compiled lock files are produced by two `bash` functions in `ci/ci.sh`:

| Function | Output | Source set |
|---|---|---|
| `compile_pip_dependencies` (`ci/ci.sh:16`) | `python/requirements_compiled.txt` | shared `python/requirements/**` files (test, cloud, docker, `ml/*`, security) |
| `compile_313_pip_dependencies` (`ci/ci.sh:82`) | `python/requirements_compiled_py3.13.txt` | py313 overrides under `python/requirements/py313/**` and `python/requirements/ml/py313/**`, falling back to shared files |

### How to invoke

```bash
# Default-Python lock (currently py3.10/3.11/3.12 generic)
ci/ci.sh compile_pip_dependencies

# py3.13 lock (uses py313 override directories)
ci/ci.sh compile_313_pip_dependencies

# Custom output filename (rare)
ci/ci.sh compile_pip_dependencies my_custom_lock.txt
```

Run inside an environment matching the target Python version (the function `pip install`s `pip-tools==7.4.1` and `wheel==0.45.1` itself). aarch64/arm64 is a no-op — the function returns early because not all pinned packages have aarch64 wheels.

### What it does

Both functions wrap `pip-compile` (pip-tools 7.4.1) with the same flags:

```
pip-compile --verbose --resolver=backtracking \
  --pip-args --no-deps --strip-extras --no-header \
  --unsafe-package ray --unsafe-package pip --unsafe-package setuptools \
  -o "python/$TARGET" \
  <list of source requirement files>
```

Then two post-process `sed` passes on the output:
1. `sed -i "/@ file/d"` — strips local file:// install lines.
2. `sed -i -E 's/==([\.0-9]+)\+[^\b]*cpu/==\1/g'` — strips `+cpu` / `+pt20cpu` device-tag suffixes that the resolver inserts (otherwise `pip install` later complains about irresolvable constraints).

The function `pip install`s `numpy` and `torch` before compilation. **Why:** `pip-compile` runs with `--pip-args --no-deps`, but it still has to extract each candidate's own `install_requires` to feed the resolver. For legacy sdists (no wheel, no PEP 517 `pyproject.toml`), the only way to get metadata is to execute `setup.py egg_info` — which runs the package's `setup.py` as Python. If that `setup.py` does `import numpy` / `import torch` at module level (common for packages using `numpy.distutils` or `torch.utils.cpp_extension` for C-extension config), the import has to succeed or pip-compile aborts. pip-compile doesn't sandbox these in an isolated PEP 517 build env, so the calling Python must already have those modules installed. `dragonfly-opt` was the canonical offender (no longer in the tree); the preinstall stays because the same shape of failure recurs whenever a new sdist-only dep with imperative `setup.py` imports gets added.

### Source file lists

If you add a brand-new source requirement file under `python/requirements/**`, it will NOT be picked up automatically — you must extend the `pip-compile` source list inside the relevant function in `ci/ci.sh`. This is one of the few legitimate reasons to edit `ci/ci.sh` (see workflow rule 2). Update both `compile_pip_dependencies` and `compile_313_pip_dependencies` if the new file applies to both Python tracks; if py3.13 needs an override, place it under `python/requirements/py313/` or `python/requirements/ml/py313/` and reference the override in `compile_313_pip_dependencies` only.

### Recompile + relock everything

After editing source requirements, the full refresh is:

```bash
ci/ci.sh compile_pip_dependencies && \
ci/ci.sh compile_313_pip_dependencies && \
bazelisk run //ci/raydepsets:raydepsets -- build --all-configs
```

The compiled `requirements_compiled*.txt` files feed raydepsets as constraints (via the `remove-compiled-headers.sh` pre-hook, which copies them to `/tmp/ray-deps/` with GPU index URLs stripped).

---

## raydepsets System

### What it does

raydepsets models relationships between lock files as a directed acyclic graph (DAG), so when a dependency changes, downstream lock files regenerate in the correct order. It wraps `uv pip compile` with cross-file consistency guarantees.

### Architecture

```
YAML configs --> Config parser (workspace.py) --> Template expansion --> DAG (NetworkX DiGraph) --> Topological execution --> uv pip compile --> Lock files
```

**Source files:**
- `ci/raydepsets/raydepsets.py` — entry point
- `ci/raydepsets/cli.py` — CLI (`build` command) and `DependencySetManager` class
- `ci/raydepsets/workspace.py` — config parsing, `Depset` dataclass, `Workspace` class, template substitution

### Four operations

#### 1. `compile`
Runs `uv pip compile` to resolve dependencies from `.in`/`.txt` requirement files into a hash-verified lock file.

**Fields:** `requirements` (input files), `constraints` (version constraint files), `packages` (inline package specs via stdin), `output` (lock file path)

```yaml
- name: ray_img_depset_${PYTHON_SHORT}
  operation: compile
  requirements:
    - python/deplocks/ray_img/ray_dev.in
  constraints:
    - /tmp/ray-deps/requirements_compiled_py${PYTHON_VERSION}.txt
  output: python/deplocks/ray_img/ray_img_py${PYTHON_SHORT}.lock
  append_flags:
    - --python-version=${PYTHON_VERSION}
    - --unsafe-package ray
    - --python-platform=linux
  build_arg_sets: [py310, py311, py312, py313]
  pre_hooks:
    - ci/raydepsets/pre_hooks/build-placeholder-wheel.sh
    - ci/raydepsets/pre_hooks/remove-compiled-headers.sh ${PYTHON_VERSION}
```

#### 2. `subset`
Extracts a subset of already-resolved packages from another depset's lock file. Validates all requested requirements exist in the source.

**Fields:** `source_depset`, `requirements`, `output`

```yaml
- name: ray_base_deps_${PYTHON_SHORT}
  operation: subset
  source_depset: ray_base_extra_testdeps_${PYTHON_SHORT}
  requirements:
    - docker/base-deps/requirements.in
  output: python/deplocks/base_deps/ray_base_deps_py${PYTHON_VERSION}.lock
```

#### 3. `expand`
Combines multiple depsets into one, optionally adding new requirements. Recursively collects all transitive requirements from referenced depsets and recompiles.

**Fields:** `depsets` (depset names to combine), `requirements`, `constraints`, `output`

```yaml
- name: compiled_ray_llm_test_depset_${PYTHON_VERSION}_${CUDA_CODE}
  operation: expand
  depsets:
    - ray_base_test_depset_${PYTHON_VERSION}_${CUDA_CODE}
  requirements:
    - python/requirements/llm/llm-requirements.txt
    - python/requirements/llm/llm-test-requirements.txt
  constraints:
    - python/deplocks/llm/ray_test_${PYTHON_VERSION}_${CUDA_CODE}.lock
  output: python/deplocks/llm/rayllm_test_${PYTHON_VERSION}_${CUDA_CODE}.lock
```

#### 4. `relax`
Removes specified packages from another depset's lock file. Does NOT re-resolve — simple removal. Use with caution; can create inconsistent environments.

**Fields:** `source_depset`, `packages`, `output`

```yaml
- name: relaxed_data_ci_depset_${PYTHON_SHORT}
  operation: relax
  source_depset: data_base_ci_depset_${PYTHON_SHORT}
  packages:
    - pyarrow
    - numpy
    - datasets
  output: python/deplocks/ci/relaxed_data-ci_depset_py${PYTHON_VERSION}.lock
```

### Config file format

Configs use the `.depsets.yaml` extension and live in `ci/raydepsets/configs/`.

**Top-level keys:**
- `build_arg_sets` (optional) — template variable sets for matrix expansion
- `depsets` — list of dependency set definitions

**Template variables** use `${VARIABLE_NAME}`. When a depset lists multiple `build_arg_sets`, it expands into one concrete depset per set.

| Field | Type | Description |
|---|---|---|
| `name` | string | Unique identifier (supports `${VAR}` substitution) |
| `operation` | string | `compile`, `subset`, `expand`, or `relax` |
| `output` | string | Output lock file path relative to workspace root |
| `build_arg_sets` | list | Which build arg sets to expand with |
| `append_flags` | list | Additional flags passed to `uv pip compile` |
| `override_flags` | list | Flags that replace matching defaults |
| `pre_hooks` | list | Shell commands to run before execution |
| `include_setuptools` | bool | Allow setuptools in output (default: `false`) |

**YAML anchors** are supported for DRY config:
```yaml
.common_settings: &common_settings
  append_flags:
    - --python-version=${PYTHON_VERSION_STR}
    - --unsafe-package ray
  build_arg_sets: [cpu, cu128]

depsets:
  - name: my_depset
    <<: *common_settings
    operation: compile
    requirements: [requirements.txt]
    output: output.lock
```

### Default `uv pip compile` flags

Applied automatically to every `compile` call:
```
--no-header
--generate-hashes
--index-strategy unsafe-best-match
--no-strip-markers
--emit-index-url
--emit-find-links
--quiet
--unsafe-package setuptools  (unless include_setuptools: true)
```

### Pre-hooks

Shell scripts that run before a depset executes. Modeled as nodes in the DAG.

- `ci/raydepsets/pre_hooks/build-placeholder-wheel.sh` — builds a placeholder Ray wheel so `uv pip compile` can resolve Ray as a dependency
- `ci/raydepsets/pre_hooks/remove-compiled-headers.sh ${PYTHON_VERSION}` — copies `requirements_compiled` to `/tmp/ray-deps/` and strips GPU index URLs (`--extra-index-url`, `--find-links`) so they don't leak into CPU lock files

### Existing config files

| Config | Purpose |
|---|---|
| `rayimg.depsets.yaml` | Core Ray Docker images (`ray_img_depset_*`, `ray_base_deps_*`, `ray_base_extra_*`, `ray_base_slim_*`, CPU/GPU/ML/LLM variants) |
| `rayllm.depsets.yaml` | RayLLM dependencies |
| `ci_data.depsets.yaml` | Ray Data CI tests (pyarrow latest/v9/nightly variants, mongo) |
| `ci_serve.depsets.yaml` | Ray Serve CI tests |
| `data_test.depsets.yaml` | Data test dependencies |
| `docs.depsets.yaml` | Documentation build dependencies |
| `llm_release_tests.depsets.yaml` | LLM release benchmarks |
| `release_compiled_graph_gpu_cu130.depsets.yaml` | Compiled graph GPU tests |
| `release_multimodal_inference_benchmarks_tests.depsets.yaml` | Multimodal benchmarks (audio, embedding, image, video) |

### Docker image hierarchy

The Ray Docker image layer structure (from `rayimg.depsets.yaml`):

```
ray_img_depset_* (compile: core Ray image deps)
  ├── ray_base_slim_* (expand: minimal slim image)
  ├── ray_base_extra_testdeps_* (expand: full test deps, CPU)
  │     ├── ray_base_deps_* (subset: first Docker layer)
  │     └── ray_base_extra_* (subset: second Docker layer)
  ├── ray_base_extra_testdeps_gpu_* (expand: GPU test deps with CUDA)
  ├── ray_base_extra_testdeps_llm_cuda_* (expand: LLM+CUDA test deps)
  ├── ray_ml_base_extra_testdeps_cuda_* (expand: ML+CUDA test deps)
  └── ray_base_deps_tpu_* (expand: TPU deps)
```

Cross-config dependencies are supported — e.g., `ci_data.depsets.yaml` references `ray_img_depset_*` from `rayimg.depsets.yaml`.

---

## Marker preservation (pip-compile, py313 / multi-Python locks)

`pip-compile` (pip-tools 7.4.1, the version `ci/ci.sh` uses) **strips `python_version` markers** from output pins UNLESS the source declaration is **an exact-version pin with the marker attached**. This matters because `requirements_compiled_py3.13.txt` is consumed as a constraint at multiple `--python-version` targets via uv.

**What works (markers survive):**
```
onnxruntime==1.18.0 ; ... and python_version <= '3.10'
onnxruntime==1.24.4 ; ... and python_version > '3.10'
```
Both exact pins, each with a marker. pip-compile at py3.11 evaluates the first as false → dropped; second as true → kept. Output retains the marker. When uv consumes the lock as constraint at `--python-version=3.10`, the marker is false → constraint skipped → resolver picks an older version from the source `.in` / `.txt`.

**What does NOT work (markers get stripped):**
```
scipy<1.16 ; python_version < '3.11'
scipy ; python_version >= '3.11'
```
A range cap + an unconstrained entry. Because scipy is also pulled transitively by many other packages with no marker, the consolidated pin loses the marker. Output: bare `scipy==1.17.1`, which then forces py3.10 depset compiles to fail (no cp310 wheel).

**Fix pattern — two exact pins:**
```
scipy==1.15.3 ; python_version < '3.11'
scipy==1.17.1 ; python_version >= '3.11'
```
Even though only one branch wins at compile time, the winning exact pin survives the transitive merge because its `==A.B.C` is more specific than any transitive range.

**Where to put the pins:**
- Broad cross-cutting compat → `python/requirements.txt`
- ML-specific compat → `python/requirements/ml/py313/ml-requirements.txt`
- Test-stack compat → `python/requirements/py313/test-requirements.txt`
- Don't pin in `python/requirements/ml/py313/dl-cpu-requirements.txt` for things unrelated to CPU torch.

**Classes of cliff to watch for when bumping py313 lock:**
1. Dropped cp310 wheels (most common; detectable via PyPI `Requires-Python` ≥ 3.11).
2. Transitive upper bounds from py<3.11-only deps (e.g. `tensorflow-metadata==1.17.3` caps `protobuf<=6.32` on py<3.11; not auto-detectable from `Requires-Python`).
3. Transitive extras that pull new deps in newer versions (e.g. `jsonschema==4.25+` adds `rfc3987-syntax` to its `format-nongpl` extra, which pulls `lark==1.3.1` and clashes with vllm's `lark==1.2.2`).
4. CPU-compile transitive pulls that conflict with GPU depsets (e.g. xgboost's unpinned `nvidia-nccl-cu12` can resolve to a version that conflicts with cu128 torch's pinned nccl — fix by pinning `nvidia-nccl-cu12==<cu128-matching-version>` in `dl-cpu-requirements.txt`).

---

## Commands

### Build all depsets from a config
```bash
bazelisk run //ci/raydepsets:raydepsets -- build ci/raydepsets/configs/rayimg.depsets.yaml
```

### Build a single named depset (and its dependencies)
```bash
bazelisk run //ci/raydepsets:raydepsets -- build ci/raydepsets/configs/rayimg.depsets.yaml --name ray_img_depset_313
```

### Build all configs at once
```bash
bazelisk run //ci/raydepsets:raydepsets -- build --all-configs
```

### Validate lock files are up-to-date (CI check mode)
```bash
bazelisk run //ci/raydepsets:raydepsets -- build ci/raydepsets/configs/rayimg.depsets.yaml --check
```

### Recompile everything (compiled requirements + all lock files)
```bash
ci/ci.sh compile_pip_dependencies && bazelisk run //ci/raydepsets:raydepsets -- build --all-configs
```

### Run raydepsets tests
```bash
bazel test //ci/raydepsets:test_cli
bazel test //ci/raydepsets:test_workspace
```

---

## Common workflows

### Adding a new dependency to a Ray component

1. Identify the correct source requirements file (e.g., `python/requirements/ml/py313/data-requirements.txt` for Ray Data on py313).
2. Add the package with appropriate version bounds (`>=min,<max`). Use the dual-exact-pin-with-markers pattern if the version differs across Python versions.
3. Recompile monorepo deps: `ci/ci.sh compile_pip_dependencies`.
4. Rebuild affected lock files: `bazelisk run //ci/raydepsets:raydepsets -- build ci/raydepsets/configs/<relevant>.depsets.yaml`.
5. Verify with `--check`.

### Creating a new CI test environment

1. Create a `.in` file listing the extra packages.
2. Create or edit a `.depsets.yaml` config in `ci/raydepsets/configs/`.
3. Choose the right base depset to expand from (usually `ray_img_depset_*` for CPU, or a GPU variant).
4. Define the depset with `operation: expand`, referencing the base and your new requirements.
5. Output to `python/deplocks/ci/<name>.lock` or appropriate path.
6. Build and verify with `bazelisk run //ci/raydepsets:raydepsets -- build ci/raydepsets/configs/<your-config>.depsets.yaml`.

### Debugging a dependency conflict

1. Read the failing lock file and the error from `uv pip compile`.
2. Check constraints — the constraints file pins versions; conflicts arise when a new requirement is incompatible.
3. Decide whether the right fix is a source-file pin (preferred), a marker-gated dual pin, or — as a last resort — a `relax` on the base depset.
4. Check cross-config dependencies — a depset in one config may depend on depsets from another config.
5. Use `--name` to rebuild just the failing depset for faster iteration.

### Upgrading a package across all lock files

1. Update the version in the source requirements file.
2. Recompile: `ci/ci.sh compile_pip_dependencies`.
3. Rebuild: `bazelisk run //ci/raydepsets:raydepsets -- build --all-configs`.
4. Review diffs in the generated lock files to verify the upgrade propagated.

---

## Patterns and gotchas

- **CUDA variants:** GPU depsets typically use `--index https://download.pytorch.org/whl/<cuda_code>` to pull PyTorch from the correct CUDA index.
- **Platform targeting:** Use `--python-platform=linux` or `--python-platform=x86_64-manylinux_2_31` for Linux-specific resolution.
- **`--unsafe-package ray`:** Always include so `uv` doesn't try to resolve Ray from PyPI (we use a local build).
- **Constraint files at `/tmp/ray-deps/`:** Created by `remove-compiled-headers.sh`; GPU-stripped versions of `requirements_compiled`.
- **`include_setuptools: true`:** Only set for base-deps and TPU depsets that need setuptools at runtime.
- **Cross-config references:** A depset in `ci_data.depsets.yaml` can reference `ray_img_depset_*` defined in `rayimg.depsets.yaml` because all configs are loaded under `--all-configs` (or pulled in transitively when one config's depset depends on a node from another).
- **Lock file output paths:** Docker image deps go to `python/deplocks/`; release test deps go to `release/ray_release/byod/`.
