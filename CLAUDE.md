# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Ray is a unified framework for scaling AI and Python applications. It consists of:
- **Ray Core**: Distributed runtime with Tasks (stateless functions), Actors (stateful workers), and Objects (immutable distributed values)
- **Ray Libraries**: Data (datasets), Train (distributed training), Tune (hyperparameter tuning), Serve (model serving), RLlib (reinforcement learning)

## Build Commands

### Building Ray from Source

```bash
# Install build dependencies
pip install -r python/requirements.txt

# Build with Bazel and install
cd python
pip install -e . -c requirements_compiled.txt
```

Environment variables for build:
- `RAY_DEBUG_BUILD=debug|asan|tsan` - Debug/sanitizer builds
- `SKIP_BAZEL_BUILD=1` - Skip Bazel build (use pre-built artifacts)
- `RAY_DISABLE_EXTRA_CPP=1` - Disable C++ extras

### Building C++ Components Only

```bash
bazel build //cpp:all
bazel run //cpp:gen_ray_cpp_pkg
```

## Testing

### Running Python Tests

```bash
# Run a single test file
pytest python/ray/tests/test_basic.py -v

# Run a specific test
pytest python/ray/tests/test_basic.py::test_function_name -v

# Run tests with Ray cluster (most tests need this)
pytest python/ray/data/tests/test_dataset.py -v
```

### Common Test Fixtures (from conftest.py)

- `ray_start_regular_shared` - Module-scoped shared Ray cluster (preferred, faster)
- `ray_start_regular` - Function-scoped Ray cluster (restarts after each test)
- `shutdown_only` - Just shuts down Ray after test (use with `ray.init()` in test)

### Running C++ Tests

```bash
bazel test --config=ci //src/ray/core_worker:all
bazel test --config=ci //cpp:all --build_tests_only
```

### Running Lint

```bash
# Run all pre-commit hooks
./ci/lint/lint.sh pre_commit

# Individual lint commands
./ci/lint/lint.sh clang_format
./ci/lint/lint.sh code_format
```

## Code Architecture

### Directory Structure

```
ray/
├── python/ray/           # Python package
│   ├── _raylet.pyx       # Cython bridge to C++ (critical integration point)
│   ├── data/             # Ray Data
│   ├── train/            # Ray Train
│   ├── tune/             # Ray Tune
│   ├── serve/            # Ray Serve
│   ├── autoscaler/       # Cluster autoscaling
│   └── tests/            # Python tests
├── src/ray/              # C++ core implementation
│   ├── raylet/           # Node manager (local scheduling, workers)
│   ├── gcs/              # Global Control Service (metadata, coordination)
│   ├── core_worker/      # Worker process runtime
│   ├── object_manager/   # Plasma object store
│   └── rpc/              # gRPC communication
├── rllib/                # Ray RLlib
├── cpp/                  # C++ API
└── ci/                   # CI scripts
```

### Key Architectural Concepts

1. **Two-level Scheduling**:
   - GCS (Global): Actor placement, cluster-level decisions
   - Raylet (Local): Task scheduling on node resources

2. **Python-C++ Bridge**: `python/ray/_raylet.pyx` is the Cython interface connecting Python APIs to C++ runtime

3. **Object Store**: Plasma-based shared memory for zero-copy data access within a node

4. **Reference Counting**: Distributed reference counting tracks object lifetimes across the cluster

### Test Organization

- Unit tests: `python/ray/*/tests/unit/` (prefer these - fast, no cluster needed)
- Integration tests: `python/ray/*/tests/` (require Ray cluster)
- C++ tests: `src/ray/*/tests/`

## Linting and Code Style

- Python: Ruff (configured in `pyproject.toml`), Black for formatting
- C++: clang-format
- Build files: Buildifier

Run `pre-commit run --all-files` for full lint check.

## Important Files

- `python/setup.py` - Python package build configuration
- `.bazelrc` - Bazel build configurations (ci, debug, asan, tsan)
- `python/requirements_compiled.txt` - Pinned dependencies for reproducible builds
- `pyproject.toml` - Ruff/linting configuration

## CI Infrastructure

### Wanda (Container Image Builder)

Wanda is Ray's container image build system. Key concepts:

- **Wanda YAML files** (`ci/docker/*.wanda.yaml`): Define container images with `name`, `froms`, `dockerfile`, `build_args`, `srcs`
- **Image naming**: The `name:` field supports env var substitution (e.g., `name: "manylinux$JDK_SUFFIX"`)
- **Wanda cache**: Built images are stored at `{RAYCI_WORK_REPO}:{RAYCI_BUILD_ID}-{wanda_name}`
  - Default `RAYCI_WORK_REPO`: `029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp`

### RayCI Pipeline Files

Pipeline files are in `.buildkite/` with `.rayci.yml` extension:

```yaml
group: my-group
steps:
  # Wanda build step - builds and caches a container image
  - name: my-image           # Step name (used in depends_on)
    wanda: ci/docker/my.wanda.yaml
    env:
      MY_VAR: "value"        # Passed to wanda build

  # Command step - runs commands in a container
  - label: "My Job"
    depends_on:
      - my-image             # Wait for wanda build
      - forge                # Common build environment
    job_env: forge           # Run IN this container (from wanda cache)
    commands:
      - bazel run //my:target
```

Key fields:
- `name:` - Step identifier for `depends_on` references
- `wanda:` - Path to wanda YAML file (makes this a build step)
- `depends_on:` - Wait for these steps to complete first
- `job_env:` - Container image to run commands in (must match a wanda `name:`)
- `instance_type:` - Machine size (small, medium, large, gpu-large)

### Crane (Container Registry Tool)

`ci/ray_ci/automation/crane_lib.py` wraps the crane binary for container operations:
- `call_crane_copy(source, dest)` - Copy image between registries
- `call_crane_manifest(tag)` - Check if image exists (return_code 0 = exists)
- `call_crane_index(name, tags)` - Create multi-arch manifest

### Docker Hub Authentication

```bash
# Via bazel (in CI)
bazel run //.buildkite:copy_files -- --destination docker_login

# The copy_files.py script authenticates via AWS API Gateway
```

### Common CI Patterns

```yaml
# Build image, then use it
- name: my-builder
  wanda: ci/docker/builder.wanda.yaml

- label: "Run tests"
  depends_on: my-builder
  job_env: my-builder        # Run inside the built image
  commands:
    - pytest tests/
```
