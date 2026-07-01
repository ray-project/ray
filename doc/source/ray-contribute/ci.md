---
myst:
  html_meta:
    description: "Explains the continuous integration workflow on Ray pull requests, including the microcheck default test set, how to add tests to it, and the full suite that runs at merge time. Read this to understand which tests run on your PR and how to trigger more."
---

# CI Testing Workflow on PRs

This guide helps contributors to understand the Continuous Integration (CI) workflow on a PR. Here CI stands for the automated testing of the codebase on the PR.

## `microcheck`: default tests on your PR

With every commit on your PR, by default, we'll run a set of tests called `microcheck`.

These tests are designed to be 90% accurate at catching bugs on your PR while running only 10% of the full test suite. As a result, microcheck typically finishes twice as fast and twice cheaper than the full test suite. Some of the notable features of microcheck are:

* If a new test is added or an existing test is modified in a pull request, microcheck will ensure these tests are included.
* You can manually add more tests to microcheck by including the following line in the body of your git commit message: `@microcheck TEST_TARGET01 TEST_TARGET02 ....`. This line must be in the body of your message, starting from the second line or below (the first line is the commit message title). For example, here is how I manually add tests in my pull request:

  ```
  // git command to add commit message
  git commit -a -s

  // content of the commit message
  run other serve doc tests

  @microcheck //doc:source/serve/doc_code/distilbert //doc:source/serve/doc_code/object_detection //doc:source/serve/doc_code/stable_diffusion

  Signed-off-by: can <can@anyscale.com>
  ```

If microcheck passes, you'll see a green checkmark on your PR. If it fails, you'll see a red cross. In either case, you'll see a summary of the test run statuses in the github UI.

## Additional tests at merge time

In this workflow, to merge your PR, simply click on the Enable auto-merge button (or ask a committer to do so). This will trigger additional test cases, and the PR will merge automatically once they finish and pass.

Alternatively, you can also add a `go` label to manually trigger the full test suite on your PR (be mindful that this is less recommended but we understand you know best about the need of your PR). While we anticipate this will be rarely needed, if you do require it constantly, please let us know. We are continuously improving the effectiveness of microcheck.

## Running the API consistency check locally

The `doc: check API doc consistency` premerge step (the `api_policy_check` function in `ci/lint/lint.sh`) runs `ci/ray_ci/doc/cmd_check_api_discrepancy.py`, which compares each team's documented API surface against the annotated (`@PublicAPI`/`@DeveloperAPI`/`@Deprecated`) surface in the code. Because it introspects the **live** code surface, it needs an environment where Ray and all the libraries it checks (`ray.data`, `ray.serve`, `ray.train`, `ray.tune`, `ray.rllib`, and the LLM submodules `ray.data.llm`/`ray.serve.llm`) import successfully. This is the opposite of the docs *build* environment, which deliberately does not install Ray (see [Building the Ray documentation](docs.md)).

### The faithful environment: the docbuild image

CI runs this check inside the `docbuild` image (`ci/docker/doc.build.Dockerfile`), which layers the docs dependency lock (`python/deplocks/docs/docbuild_depset_py3.11.lock`) on top of the prebuilt `ray-core` and `oss-ci-base_build` base images, then installs Ray itself without dependencies (`pip install -e "python[all]" --no-deps`). The dependency lock alone is **not** a complete environment — the ML libraries the walk imports (for example `pandas`, `torch`, and `transformers`) come from the base images, and the lock is pinned to Linux wheels. So the only fully faithful local reproduction is the Linux docbuild image (via Docker) or a CI run on your branch.

### A lightweight local approximation

For quick iteration on the checker itself, a Python 3.11 virtual environment with a nightly Ray wheel plus the import-time dependencies of the checked surfaces is usually enough:

```bash
uv venv --python 3.11 ~/.virtualenvs/ray-apiref
uv pip install --python ~/.virtualenvs/ray-apiref/bin/python \
  "https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp311-cp311-macosx_11_0_arm64.whl"
# Dependencies the walked surfaces need at import time (no vllm/sglang required):
uv pip install --python ~/.virtualenvs/ray-apiref/bin/python \
  pandas pyarrow numpy starlette fastapi uvicorn pydantic aiohttp grpcio transformers

# Run the check against your checkout (PYTHONPATH so the checker imports from your tree):
PYTHONPATH="$(pwd)" ~/.virtualenvs/ray-apiref/bin/python \
  ci/ray_ci/doc/cmd_check_api_discrepancy.py "$(pwd)" serve
```

Pass a single team (`core`, `data`, `serve`, `train`, `tune`, `rllib`) or omit it to check all. `train`, `tune`, and `rllib` additionally need their deep-learning backends (`torch`/`tensorflow`) installed to import.

```{warning}
A nightly wheel is built at a different commit than your checkout, so its code surface can drift from your doc tree (for example a function renamed after the nightly was cut shows up as a spurious mismatch). For a definitive result, match the Ray build to your checkout commit, or rely on the CI run in the docbuild image. Use the local environment to iterate, not to sign off.
```
