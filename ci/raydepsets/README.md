# raydepsets

A dependency lock file management tool for Ray CI pipelines. It
maintains consistency relationships among lock files — ensuring that
when a dependency is updated, all related lock files are regenerated
together in the correct order. Built on top of
[`uv pip compile`](https://docs.astral.sh/uv/pip/compile/), it
generates reproducible, hash-verified lock files across multiple
Python versions, platforms, and CUDA variants.

## Why

Ray's CI builds containers and test environments for many
combinations of Python version, platform (Linux x86_64, macOS ARM),
and GPU support (CPU, e.g. CUDA 12.8). Each combination needs a
locked, reproducible set of dependencies, and many of these lock
files have consistency relationships with each other — for example,
a test environment's lock file must be a strict superset of the base
image it runs on, and all CUDA variants for a given Python version
must agree on common package versions. A single `uv pip compile`
call can produce one lock file, but it has no awareness of these
cross-file constraints. When a dependency is updated, all downstream
lock files need to be regenerated in the right order to stay
consistent.

raydepsets solves this by:
- Modeling the relationships between lock files as a dependency
  graph, so that updating one file automatically propagates to all
  dependents
- Supporting three composable operations (**compile**, **subset**,
  **expand**) to express how lock files derive from each other
- Defining dependency sets declaratively in YAML with template
  variables for matrix builds
- Automatically resolving execution order via topological sort
- Validating that committed lock files are up-to-date and mutually
  consistent in CI (`--check` mode)

## Directory Structure

```
ci/raydepsets/
├── raydepsets.py           # Entry point
├── cli.py                  # CLI and DependencySetManager
├── workspace.py            # Config parsing and data models
├── BUILD.bazel             # Bazel build targets
├── configs/                # Production YAML configs
│   ├── rayimg.depsets.yaml
│   ├── rayllm.depsets.yaml
│   ├── data_test.depsets.yaml
│   ├── docs.depsets.yaml
│   └── ...
├── pre_hooks/              # Shell scripts run before compilation
│   ├── build-placeholder-wheel.sh
│   └── remove-compiled-headers.sh
└── tests/
    ├── test_cli.py
    ├── test_workspace.py
    ├── utils.py
    └── test_data/
```

## Usage

raydepsets is built and run via Bazel:

```bash
# Build all depsets in a config
bazelisk run //ci/raydepsets:raydepsets -- build ci/raydepsets/configs/rayimg.depsets.yaml

# Build a single named depset (and its dependencies)
bazelisk run //ci/raydepsets:raydepsets -- build ci/raydepsets/configs/rayimg.depsets.yaml --name ray_img_depset_313

# Build all configs at once
bazelisk run //ci/raydepsets:raydepsets -- build --all-configs

# Validate that lock files are up-to-date (used in CI)
bazelisk run //ci/raydepsets:raydepsets -- build ci/raydepsets/configs/rayimg.depsets.yaml --check
```

### CLI Options

| Option | Description |
|---|---|
| `CONFIG_PATH` | Path to a `.depsets.yaml` config file (default: `ci/raydepsets/configs/*.depsets.yaml`) |
| `--workspace-dir` | Workspace root directory (default: `$BUILD_WORKSPACE_DIRECTORY`) |
| `--name` | Build only this depset and its dependencies |
| `--uv-cache-dir` | Cache directory for uv |
| `--check` | Validate lock files match what would be generated; exit non-zero on diff |
| `--all-configs` | Build depsets from all config files, not just the specified one |

## Configuration Format

Config files use the `.depsets.yaml` extension and contain two top-level keys:

### `build_arg_sets` (optional)

Defines template variable sets for matrix expansion. Each key maps to a dictionary of variable substitutions:

```yaml
build_arg_sets:
  py311:
    PYTHON_VERSION: "3.11"
    PYTHON_SHORT: "311"
  py312:
    PYTHON_VERSION: "3.12"
    PYTHON_SHORT: "312"
```

Variables are referenced in depset fields using `${VARIABLE_NAME}` syntax. When a depset lists multiple `build_arg_sets`, it is expanded into one depset per set.

### `depsets`

A list of dependency set definitions. Each depset has these common fields:

| Field | Type | Description |
|---|---|---|
| `name` | string | Unique identifier (supports `${VAR}` substitution) |
| `operation` | string | One of `compile`, `subset`, `expand` |
| `output` | string | Output lock file path relative to workspace root |
| `build_arg_sets` | list | Which build arg sets to expand this depset with |
| `append_flags` | list | Additional flags passed to `uv pip compile` |
| `override_flags` | list | Flags that replace matching defaults |
| `pre_hooks` | list | Shell commands to run before this depset executes |
| `include_setuptools` | bool | Allow setuptools in output (default: `false`) |

#### Operation: `compile`

Runs `uv pip compile` to resolve and lock dependencies from requirements files.

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
  build_arg_sets:
    - py310
    - py311
    - py312
    - py313
```

Additional fields: `requirements` (input requirement files), `constraints` (version constraint files), `packages` (inline package specs passed via stdin).

#### Operation: `subset`

Extracts a subset of already-resolved dependencies from another depset's lock file. Validates that all requested requirements exist in the source.

```yaml
- name: ray_base_deps_${PYTHON_SHORT}
  operation: subset
  source_depset: ray_base_extra_testdeps_${PYTHON_SHORT}
  requirements:
    - docker/base-deps/requirements.in
  output: python/deplocks/base_deps/ray_base_deps_py${PYTHON_VERSION}.lock
```

Additional fields: `source_depset` (name of the depset to subset from).

#### Operation: `expand`

Combines multiple depsets into one, optionally adding new requirements. Recursively collects all transitive requirements from referenced depsets.

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

Additional fields: `depsets` (list of depset names to combine), `requirements` (extra requirements to include), `constraints` (constraint files).

### YAML Anchors

Configs support standard YAML anchors for DRY definitions:

```yaml
.common_settings: &common_settings
  append_flags:
    - --python-version=3.11
    - --unsafe-package ray
  build_arg_sets:
    - cpu
    - cu128

depsets:
  - name: my_depset
    <<: *common_settings
    operation: compile
    requirements:
      - requirements.txt
    output: output.lock
```

## Pre-Hooks

Pre-hooks are shell scripts that run before a depset is executed. They are useful for preparing the build environment (e.g., building placeholder wheels, stripping GPU index URLs from constraint files).

```yaml
pre_hooks:
  - ci/raydepsets/pre_hooks/build-placeholder-wheel.sh
  - ci/raydepsets/pre_hooks/remove-compiled-headers.sh ${PYTHON_VERSION}
```

Pre-hooks support template variable substitution and are modeled as nodes in the dependency graph, so they execute in the correct order.

## How It Works

1. **Config loading** -- YAML configs are parsed into `Depset` dataclasses. Template variables from `build_arg_sets` are substituted, expanding one depset definition into N concrete depsets.
2. **Graph construction** -- A directed acyclic graph (NetworkX `DiGraph`) is built from depset dependencies and pre-hooks.
3. **Topological execution** -- Depsets are executed in topological order so dependencies are resolved before dependents.
4. **Lock file generation** -- Each depset calls `uv pip compile` with the appropriate flags, constraints, and requirements.
5. **Validation** (`--check` mode) -- Lock files are generated to a temp directory and compared against committed versions. Any diff causes a non-zero exit.

### Default `uv pip compile` Flags

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
