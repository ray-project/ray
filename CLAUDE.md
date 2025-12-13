# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Ray?

Ray is a unified framework for scaling AI and Python applications, consisting of:
- **Ray Core**: Distributed runtime with Tasks, Actors, and Objects as key abstractions
- **Ray AI Libraries**: Data, Train, Tune, Serve, and RLlib for ML workloads
- Language support: Python (primary), Java, C++

## Quick Reference

```bash
# Run tests
pytest python/ray/tests/test_foo.py       # Python test
bazel test //src/ray/raylet:raylet_test   # C++ test

# Build
cd python && pip install -e . --verbose   # Full build (C++ + Python)
bazel build //src/ray/raylet:raylet       # C++ component only

# Lint changed files
pre-commit run --from-ref master --to-ref HEAD

# Clean up
ray stop                                   # Stop all Ray processes
```

## Gotchas

- **Run `ray stop` before tests** - Stale processes cause "Address already in use" errors
- **Rebuild Cython after C++ changes**: `cd python && pip install -e . --verbose`
- **Use `--config=ci`** for faster incremental Bazel builds
- **Logs location**: `/tmp/ray/session_latest/logs/`
- **IMPORTANT: Use `ray_cc_library`** not `cc_library` in BUILD files
- **IMPORTANT: Use `absl::Mutex`** not `std::mutex` for thread synchronization
- **IMPORTANT: Avoid `time.sleep()` in tests** - Use `ray.wait()` and `ray.get()` instead

## Build System

Ray uses **Bazel** for C++ components and **Python setuptools** for Python packaging.

### Building Ray

```bash
# Full build (C++ and Python)
cd python
pip install -e . --verbose

# Build only Python (skip C++ rebuild if already built)
cd python
python setup.py develop

# Build C++ components with Bazel
bazel build //:ray_pkg

# Build specific C++ targets
bazel build //src/ray/raylet:raylet
bazel build //src/ray/gcs:gcs_server
bazel build //src/ray/core_worker:core_worker_lib

# Install Python proto files
bazel run //:gen_py_proto

# Refresh compile commands for clangd/IDE
bazel run //:refresh_compile_commands
```

### Build Configuration

- `.bazelrc`: Bazel build configuration (compiler flags, sanitizers, CI configs)
- `WORKSPACE`: External dependencies and repositories
- `BUILD.bazel`: Root build file (see also `src/ray/*/BUILD` files)
- `pyproject.toml`: Python project config (ruff linting, isort)
- `python/setup.py`: Python package build script

## Testing

### Python Tests

```bash
# Run tests with pytest (most common for development)
cd python
pytest ray/tests/test_actor.py
pytest ray/tests/test_actor.py::test_actor_creation
pytest ray/tests/test_actor.py -v -s  # verbose with stdout

# Run specific test suites
pytest ray/data/tests/
pytest ray/serve/tests/

# Run with specific markers
pytest -m "not slow"
```

### C++ Tests

```bash
# Run C++ tests with Bazel
bazel test //src/ray/raylet:raylet_test
bazel test //src/ray/core_worker:core_worker_test
bazel test //src/ray/gcs:gcs_server_test

# Run all tests in a package
bazel test //src/ray/raylet/...

# Run with specific configs
bazel test --config=ci //src/ray/...
bazel test --config=asan //src/ray/raylet:raylet_test
bazel test --config=tsan //src/ray/core_worker:core_worker_test
```

### Test Organization

- Python tests: `python/ray/tests/` (core), `python/ray/*/tests/` (per-library)
- C++ tests: Co-located with source in `src/ray/*/` directories
- Test fixtures: `python/ray/tests/conftest.py`
- Default pytest timeout: 180 seconds (configured in `pytest.ini`)

### Test Writing Guidelines

- **IMPORTANT: Avoid `time.sleep()` in tests** - Instead, synchronize by waiting on tasks
  - Use `ray.wait()` and `ray.get()` to wait for tasks to complete before verification
  - Example: Use wait-then-get-and-verify approach instead of sleep-then-verify
- **Update tests incrementally**: When implementing code changes step-by-step, update corresponding tests at each step

## Code Architecture

### High-Level Layers

```
┌─────────────────────────────────────────────────┐
│  AI Libraries (Train, Tune, Serve, Data, RLlib) │
├─────────────────────────────────────────────────┤
│  Ray Core API (ray.remote, Actors, Objects)     │
├─────────────────────────────────────────────────┤
│  Python Runtime (Worker, Client)                │
├─────────────────────────────────────────────────┤
│  C++ Core (_raylet.so via Cython)               │
├─────────────────────────────────────────────────┤
│  Distributed Runtime (Raylet, GCS, ObjectMgr)   │
└─────────────────────────────────────────────────┘
```

### Key C++ Components

Located in `src/ray/`:

- **raylet/** - Local scheduler and resource manager per node
  - Schedules tasks and actors on the node
  - Manages local resources (CPU, GPU, memory)
  - Implements the Node Manager

- **gcs/** (Global Control Store) - Centralized metadata service
  - Cluster-wide state management
  - Actor directory, placement groups
  - Resource tracking and job management
  - Backed by Redis (transitioning to built-in storage)

- **core_worker/** - Task execution engine
  - Runs in each worker process (Python, Java, C++)
  - Handles task submission, execution, and dependencies
  - Object store interface and reference counting
  - Direct actor calls and streaming

- **object_manager/** - Distributed object storage
  - Transfer objects between nodes
  - Built on top of Plasma store (shared memory)
  - Implements distributed reference counting

- **rpc/** - gRPC service definitions and stubs
  - Protobuf definitions in `protobuf/`
  - Generated code for inter-component communication

- **common/** - Shared utilities and data structures
  - IDs (TaskID, ActorID, ObjectID, etc.)
  - Status codes and error handling
  - Ray config and constants

For detailed C++ file reference, state machines, and component relationships, see [CPP_ARCHITECTURE.md](CPP_ARCHITECTURE.md).

### Key Python Components

Located in `python/ray/`:

- **__init__.py** - Main API entry point (`ray.init()`, `ray.remote()`, `ray.get()`, `ray.put()`)
- **actor.py** - Actor implementation and decorators
- **remote_function.py** - Task/remote function implementation
- **runtime_context.py** - Access to runtime information (job_id, node_id, etc.)
- **_private/worker.py** - Worker process implementation and lifecycle
- **_private/client_mode_hook.py** - Ray Client for remote cluster access
- **_private/ray_constants.py** - Python-side constants
- **_raylet.pyx** - Cython binding to C++ core

For detailed Python architecture, design patterns, and file reference, see [PY_ARCHITECTURE.md](PY_ARCHITECTURE.md).

### AI Libraries (out of scope for this guide)

See library-specific docs: `python/ray/data/`, `python/ray/train/`, `python/ray/tune/`, `python/ray/serve/`, `rllib/`

### Important Design Patterns

1. **Task Execution Flow**:
   - User calls `ray.remote(func).remote(args)` in Python
   - Python worker submits task via `core_worker->SubmitTask()`
   - Core worker contacts local Raylet for scheduling
   - Raylet schedules task (local or remote via GCS)
   - Worker executes task, stores result in object store
   - Caller retrieves result via `ray.get()`

2. **Actor Lifecycle**:
   - Actor creation: GCS assigns actor to a node
   - Raylet on that node starts worker process for actor
   - Actor handle allows direct method calls (bypassing Raylet)
   - GCS tracks actor state (pending, alive, dead)

3. **Object Management**:
   - Objects stored in local Plasma store (shared memory)
   - Large objects: Raylet manages transfer between nodes
   - Reference counting: Distributed protocol prevents premature deletion
   - Spilling: Large objects can spill to disk

4. **Hierarchical IDs**:
   - JobID -> TaskID -> ObjectID (forms lineage tree)
   - Used for scheduling, fault tolerance, and debugging

## Development Workflow

### Making Changes

1. **C++ changes**:
   - Edit files in `src/ray/`
   - Rebuild: `bazel build //src/ray/raylet:raylet` (or specific target)
   - Run tests: `bazel test //src/ray/raylet:raylet_test`
   - For development iteration, prefer building specific targets over `//:ray_pkg`

2. **Python changes**:
   - Edit files in `python/ray/`
   - Changes take effect immediately if using `pip install -e .`
   - Run tests: `pytest python/ray/tests/test_xyz.py`

3. **Changes to both**:
   - If you modify `.pyx` or C++ headers used by Python:
     ```bash
     cd python
     pip install -e . --verbose  # Rebuilds Cython bindings
     ```

4. **Protobuf changes**:
   - Edit `.proto` files in `src/ray/protobuf/`
   - Rebuild: `bazel build //src/ray/protobuf:all`
   - Install Python protos: `bazel run //:gen_py_proto`

### Linting and Formatting

Ray uses **pre-commit** hooks for linting and formatting. Install pre-commit first:

```bash
pip install pre-commit
pre-commit install                           # Auto-run on git commit
cp ci/lint/pre-push .git/hooks/pre-push      # Auto-run on git push
```

```bash
# Lint only changed files (recommended for development)
pre-commit run --from-ref master --to-ref HEAD

# Run all pre-commit hooks on all files
pre-commit run --all-files

# Run specific hooks
pre-commit run ruff --all-files         # Python linting (ruff)
pre-commit run black --all-files        # Python formatting (black)
pre-commit run clang-format --all-files # C++ formatting
pre-commit run buildifier --all-files   # Bazel BUILD files

# Check C++ format (CI uses this)
./ci/lint/check-git-clang-format-output.sh

# Check Python API annotations
python ci/lint/check_api_annotations.py

# Check documentation style
pre-commit run docstyle --all-files      # Python docstring format
pre-commit run pydoclint --all-files     # Thorough pydoc checking
pre-commit run vale --all-files          # Prose style (doc/source/data/)

# Run full lint suite (as done in CI)
./ci/lint/lint.sh
```

### Common Issues

1. **"ImportError: cannot import name _raylet"**
   - Solution: Rebuild Cython extension: `cd python && pip install -e . --verbose`

2. **Bazel build fails with "undeclared inclusion"**
   - Cause: Missing dependency in BUILD file
   - Solution: Add missing dep to `deps = [...]` in the relevant BUILD target

3. **Tests fail with "Address already in use"**
   - Cause: Previous Ray instance not properly shut down
   - Solution: `ray stop` or `pkill -9 raylet && pkill -9 gcs_server`

4. **Slow Bazel builds**
   - Use `--config=ci` for faster incremental builds
   - Set `BAZEL_LIMIT_CPUS` to limit parallelism on resource-constrained machines

## Key Files Reference

### Build and Configuration
- `BUILD.bazel` - Root Bazel build file
- `.bazelrc` - Bazel configuration (compiler flags, test configs)
- `WORKSPACE` - External Bazel dependencies
- `python/setup.py` - Python package build
- `pyproject.toml` - Python tooling config (ruff, isort)
- `pytest.ini` - Pytest configuration

### C++ Entry Points
- `src/ray/raylet/main.cc` - Raylet binary entry point
- `src/ray/gcs/gcs_server/gcs_server.cc` - GCS server entry point
- `src/ray/core_worker/core_worker.cc` - Core worker implementation

### Python Entry Points
- `python/ray/__init__.py` - Main Ray API
- `python/ray/_private/worker.py` - Worker process main loop
- `python/ray/_raylet.pyx` - Cython bridge to C++

### Testing
- `python/ray/tests/conftest.py` - Pytest fixtures and configuration
- `python/ray/tests/BUILD.bazel` - Test targets

### Documentation
- `doc/` - Sphinx documentation source (RST format, follows style guide in `doc/.cursor/rules/ray-docs-style.mdc`)
- Build docs: `cd doc && make html` (output in `doc/_build/html/`)
- Live reload: `cd doc && make local` (auto-rebuilds on changes)
- Check links: `cd doc && make linkcheck`
- Serve locally: `cd doc && make show`

## Ray-Specific Conventions

### Environment Setup
- **Always activate the Ray conda environment before development**: `conda activate rayenv`
  - This ensures a compatible Python environment for Ray development

### Naming Conventions
- C++ class names: `PascalCase` (e.g., `CoreWorker`, `NodeManager`)
- C++ file names: `snake_case.cc/.h` (e.g., `core_worker.cc`)
- Python: Follow PEP 8 (`snake_case` for functions, `PascalCase` for classes)
- Bazel targets: `snake_case` (e.g., `core_worker_lib`)

### Code Style
- C++: Follow Google C++ Style Guide, enforced by clang-format
- Python: Follow PEP 8, enforced by ruff
- **IMPORTANT:** Use `ray_cc_library` (not `cc_library`) for Ray C++ libraries in BUILD files
- Add `team:*` tags to test targets for ownership tracking

### Comments
- **Prefer `//` for comments** instead of `///` in C++ code
- Write **concise comments that explain why and how**, not what
- Reserve comments for **unobvious behavior** only
- Remove unnecessary or redundant comments

### Error Handling
- C++: Use `ray::Status` for error propagation (similar to `absl::Status`)
- Python: Raise `RayError` or subclasses (`RayTaskError`, `RayActorError`, etc.)
- **IMPORTANT:** Check return statuses with `RAY_CHECK_OK(status)` or `RAY_RETURN_NOT_OK(status)`

### Logging
- C++: Use `RAY_LOG(INFO/WARNING/ERROR/FATAL)` macros
- Python: Use standard `logging` module, logger name: `ray.*`
- Set log level: `ray.init(logging_level=logging.DEBUG)`
- You can find raylet, GCS, worker and driver logs under `/tmp/ray` directory.

### Memory Management (C++)
- **Prefer `std::unique_ptr` over `std::shared_ptr`** for memory management
- Use `shared_ptr` only when shared ownership is truly necessary
- **IMPORTANT:** Prefer `absl::Mutex` over `std::mutex` for thread synchronization

### Code Design Principles
- **Minimize surface area**: Remove thin wrapper functions that don't add readability
- **Minimal constructor parameters**: Avoid adding additional constructor arguments when possible
- **Step-by-step changes**: Break down code changes into incremental steps rather than large rewrites
- **Backward compatibility**: Keep code backward compatible and succinct
- **Concise, simple scripts**: Favor minimal code over lengthy examples

## Debugging

### Python Debugging
```python
# Enable verbose logging
ray.init(logging_level=logging.DEBUG)

# Inspect Ray state
import ray
ray.nodes()           # List nodes
ray.cluster_resources()  # View resources
ray.available_resources()

# Access runtime context
ray.get_runtime_context().get_job_id()
ray.get_runtime_context().get_node_id()
```

### C++ Debugging
```bash
# Build with debug symbols
bazel build -c dbg //src/ray/raylet:raylet

# Run under gdb
gdb bazel-bin/src/ray/raylet/raylet

# Attach to running process
gdb -p $(pgrep raylet)

# Enable Ray debug logs (C++)
export RAY_BACKEND_LOG_LEVEL=debug
```

### Useful Environment Variables
- `RAY_BACKEND_LOG_LEVEL` - C++ log level (debug/info/warning/error)
- `RAY_LOG_TO_STDERR` - Print logs to stderr instead of files
- `RAY_enable_timeline` - Enable Chrome trace timeline for profiling
- `RAY_DEBUG_DISABLE_MEMORY_MONITOR` - Disable OOM monitor for debugging

## PR and Contribution Guidelines

- Add reviewer to "assignee" field when creating PR
- Use `@author-action-required` label for review feedback
- Add `test-ok` label once build passes (for contributors in ray-project org)
- Ensure tests pass: Python tests via pytest, C++ tests via bazel test
- Follow 3-retry policy for flaky tests (configured in `.bazelrc`)

## Additional Resources

- Main docs: https://docs.ray.io/
- Architecture whitepaper: https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/
- Discourse forum: https://discuss.ray.io/
- GitHub issues: https://github.com/ray-project/ray/issues
