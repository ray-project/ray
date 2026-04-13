---
name: rebuild
description: Rebuild Ray from source — determines the right build mode based on what changed
---

# Rebuild Ray

Canonical build docs: doc/source/ray-contribute/development.rst
Use the user's configured Python from CLAUDE.local.md, or fall back to `which python`.
Update this skill if any changes are detected in development.rst.

## Determine what to rebuild

1. Check what files changed: `git diff --name-only HEAD` (or ask the user)
2. Pick the appropriate build mode:
   - **Only .py files changed** (no .h, .cc, .pyx, .pxd) → Python-only rebuild (not required if installed with `pip install -e .` — editable mode auto-picks up Python changes)
   - **.h, .cc, .pyx, .pxd files changed** → C++/Cython rebuild
   - **Dashboard files changed** (python/ray/dashboard/client/) → Dashboard rebuild first
   - **First time / clean build** → Full source build

## Python-only rebuild

For changes to Tune, RLlib, Autoscaler, and most Python files.
Two approaches depending on how Ray was installed:

### If installed with `pip install -e .` (editable/full source build)
No rebuild needed — Python changes are picked up automatically.

### If installed from nightly wheel + setup-dev.py
1. Ensure a nightly wheel is installed (see doc/source/ray-contribute/development.rst
   for the correct wheel URL for the user's platform and Python version)
2. Link local files: `python python/ray/setup-dev.py`
3. Can skip specific dirs: `python python/ray/setup-dev.py -y --skip _private dashboard`

**Warning:** With setup-dev.py, don't run `pip uninstall ray` or `pip install -U`.
Instead: `rm -rf <site-packages>/ray`, reinstall wheel, re-run setup-dev.py.

## C++/Cython rebuild (after initial full build)

### Full package rebuild
```bash
bazel run //:gen_ray_pkg
```

### Build individual modules (faster)
```bash
bazel build //src/ray/raylet:raylet          # Raylet only
bazel build //src/ray/gcs:gcs_server         # GCS server only
bazel build //src/ray/core_worker:core_worker # Core worker only
bazel build //:ray_pkg                       # All C++ and Cython
```

### Build variants
- `bazel run -c fastbuild //:gen_ray_pkg` — fast build (less optimization)
- `bazel run -c dbg //:gen_ray_pkg` — debug build with symbols (for gdb)
- `bazel run -c opt //:gen_ray_pkg` — optimized build

Make permanent in `~/.bazelrc`: `build --compilation_mode=fastbuild`

## Full source build (first time)

1. If dashboard changed: `cd python/ray/dashboard/client && npm ci && npm run build && cd -`
2. Install deps: `cd python/ && pip install -r requirements.txt`
3. Build: `pip install -e . --verbose`

## Resource-constrained builds
Add to `~/.bazelrc` if machine runs out of memory:
```
build --local_ram_resources=HOST_RAM*.5 --local_cpu_resources=4
build --disk_cache=~/bazel-cache
```

## Worktree notes
- Verify build paths: `bazel info execution_root`
- If build fails, check .bazelversion matches installed Bazel
- Each worktree may need its own `pip install -e .`
- If in a worktree and bazel targets fail, ensure symlinks are correct
