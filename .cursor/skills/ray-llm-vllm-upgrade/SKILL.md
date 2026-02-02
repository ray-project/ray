---
name: llm-vllm-upgrade
description: Upgrade the vLLM library in ray-llm docker images
---

# vLLM Upgrade Skill

This skill provides detailed instructions for upgrading the vLLM dependency in Ray LLM docker images and resolving breaking changes to ensure Ray LLM CI tests succeed.

## When to Use

Use this skill when:
- Users request to upgrade the vLLM dependency in Ray LLM
- Users report docker build failures during vLLM upgrade (often with Buildkite error logs)
- Users report test failures or breaking changes after a vLLM upgrade

This skill is helpful for:
- **Upgrading vLLM and dependencies**: Updating the vLLM library and resolving dependency conflicts
- **Resolving docker build failures**: Diagnosing and fixing build issues that may arise from:
  - New vLLM dependencies not present in Ray LLM
  - CUDA version mismatches
  - Library conflicts (e.g., DeepEP, DeepGEMM, NVSHMEM)
- **Fixing breaking changes**: Addressing API changes in vLLM that affect:
  - Ray Data LLM (`python/ray/llm`)
  - Ray Serve LLM (`python/ray/llm`)
  - Release tests (`release/llm_tests`)

## Instructions

Goal: After upgrading vLLM in the ray-llm Docker image, all Ray LLM tests should pass when running the container, ensuring compatibility with Ray Data LLM, Ray Serve LLM, and release tests.

### 1. Update vLLM Dependency

- Search for `vllm[audio]` references in the codebase and upgrade to the desired version
- Update the vLLM dependency in the ray-llm Dockerfile (`docker/ray-llm/Dockerfile`)

### 2. Compile Dependencies

- Run `bash ci/ci.sh compile_pip_dependencies` and resolve any errors that occur
- Run `bash ci/compile_llm_requirements.sh` and resolve any errors that occur
- Run `bazel run //ci/raydepsets:raydepsets -- build --all-configs` and resolve any errors that occur
- When any command fails, do not rerun the ones that already succeeded. Only start with the command that failed.
- If errors occur, you can run your own commands to resolve environment conflicts or missing dependencies, such as installing Bazel or system packages. Refer to `compile-dependencies.sh` on how Bazel is installed.
- Do not modify `ci/raydepsets/pre_hooks` because it affects Ray base images. Keep changes scoped to Ray LLM images and understand how Ray LLM and base images are built before changing Dockerfiles or depsets.
- Do not modify any `.lock` files. They should be modified automatically with the commands provided above.

### 3. Upgrade vLLM locally
- Run `uv pip install vLLM==<desired_version>` to make sure vLLM is upgraded before running any representative tests.
- If vLLM upgrades pull a new library major version, update other runtime deps (and any ray-llm Dockerfile install steps) in the same environment or image to compatible versions to avoid binary import errors during tests.

### 4. Investigate Breaking Changes

- Review vLLM changes using GitHub compare:
  ```
  https://github.com/vllm-project/vllm/compare/<current_version>...<desired_version>
  ```
  Example: `https://github.com/vllm-project/vllm/compare/v0.13.0...v0.15.0`
- Determine if there are breaking changes that affect Ray LLM
- For deeper investigation, clone the vLLM repository outside the current project:
  ```bash
  git clone https://github.com/vllm-project/vllm.git
  git checkout releases/<current_version>
  git checkout releases/<desired_version>
  ```
- Compare differences and search for relevant files in the vLLM repository

### 5. Test Changes

After fixing breaking changes, run representative LLM tests:

```bash
python -m pytest -vs python/ray/llm/tests/gpu/processor/test_vllm_engine_proc.py
python -m pytest -vs python/ray/llm/tests/serve/gpu/deployments/llm/vllm/test_vllm_engine_gpu.py
```

### 6. Full CI Testing (Optional)

- For comprehensive testing, run the full LLM CI tests outlined in `.buildkite/llm.rayci.yml`
- This is usually not necessary, but study the file for context when users provide failed Buildkite commands and associated error logs