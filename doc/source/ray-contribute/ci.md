---
myst:
  html_meta:
    description: "Explains the continuous integration workflow on Ray pull requests, including the microcheck default test set, how to add tests to it, and the full suite that runs at merge time. Read this to understand which tests run on your PR and how to trigger more."
---

# CI testing workflow on PRs

This guide helps contributors understand the continuous integration (CI) workflow on a PR. Here, CI stands for automated testing of the codebase on the PR.

## `microcheck`: default tests on your PR

With every commit on your PR, by default, we'll run a set of tests called `microcheck`.

These tests are designed to be 90% accurate at catching bugs on your PR while running only 10% of the full test suite. As a result, microcheck typically finishes twice as fast and at half the cost of the full test suite. Some notable features of microcheck are:

* If a new test is added or an existing test is modified in a pull request, microcheck ensures these tests are included.
* You can manually add more tests to microcheck by including the following line in the body of your git commit message: `@microcheck TEST_TARGET01 TEST_TARGET02 ....`. This line must be in the body of your message, starting from the second line or below (the first line is the commit message title). For example, here is how I manually add tests in my pull request:

  ```
  // git command to add commit message
  git commit -a -s

  // content of the commit message
  run other serve doc tests

  @microcheck //doc:source/serve/doc_code/distilbert //doc:source/serve/doc_code/object_detection //doc:source/serve/doc_code/stable_diffusion

  Signed-off-by: can <can@anyscale.com>
  ```

If microcheck passes, you'll see a green checkmark on your PR. If it fails, you'll see a red cross. In either case, you'll see a summary of the test run statuses in the GitHub UI.

## Additional tests at merge time

`microcheck` runs on every commit, but the full test suite must pass before a PR can merge. Adding the `go` label triggers the full suite, and committers require the `go` tests to have passed before adding a PR to the merge queue.

If you're a committer, add the `go` label to your PR once it's ready, then merge after the full suite passes. Clicking **Enable auto-merge** does both in one step: it adds the `go` label and merges the PR automatically once the suite passes. Pushing a new commit disables auto-merge, so re-enable it afterward. When you review an external contributor's PR, add the `go` label for them, since they can't add it themselves.

If you're an external contributor, adding the `go` label and enabling auto-merge both require write access, so a committer runs the full suite and merges when your PR is ready.

## Running the API consistency check locally

The `doc: check API doc consistency` premerge step (the `api_policy_check` function in `ci/lint/lint.sh`) runs `ci/ray_ci/doc/cmd_check_api_discrepancy.py`, which compares each team's documented API surface against the annotated (`@PublicAPI`/`@DeveloperAPI`/`@Deprecated`) surface in the code. It imports Ray's own symbols for real, so it needs an environment where Ray installs and imports. This is the opposite of the docs *build* environment, which deliberately doesn't install Ray (see [Building the Ray documentation](docs.md)). The optional third-party backends that some surfaces pull in at import time (for example the vLLM and SGLang stack behind `ray.data.llm` and `ray.serve.llm`, or the deep-learning backends behind `ray.train`, `ray.tune`, and `ray.rllib`) don't have to be installed: the check mocks any that are absent, reading the list from `doc/source/api_mock_imports.py`, the same source of truth the docs build uses for `autodoc_mock_imports`.

### The faithful environment: the docbuild image

CI runs this check inside the `docbuild` image (`ci/docker/doc.build.Dockerfile`). The image builds on the prebuilt `ray-core` and `oss-ci-base_build` base images, which supply Ray and the real ML libraries the walk imports (for example `pandas`, `torch`, and `transformers`), then installs the docs dependency lock (`python/deplocks/docs/docbuild_depset_py<version>.lock`) on top. Because the real libraries come from the base images and the lock is pinned to Linux wheels, the only fully faithful local reproduction is the Linux docbuild image (via Docker) or a CI run on your branch.

### A lightweight local approximation

For quick iteration on the checker itself, a Python 3.11 virtual environment with a nightly Ray wheel is usually enough. Because the check mocks the absent optional backends, you generally don't need to install `pandas`, `torch`, `transformers`, and the rest by hand. Install a backend for real only when you want the check to walk that library's true surface rather than a mock.

```bash
uv venv --python 3.11 ~/.virtualenvs/ray-apiref
# The wheel URL below is macOS arm64 / Python 3.11. Adjust it for your OS, architecture, and Python version.
uv pip install --python ~/.virtualenvs/ray-apiref/bin/python \
  "https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp311-cp311-macosx_11_0_arm64.whl"
# Link your checkout's Python files over the wheel so the check walks your local @PublicAPI changes, not the wheel's:
~/.virtualenvs/ray-apiref/bin/python python/ray/setup-dev.py --yes

# Run the check against your checkout (PYTHONPATH so the checker imports from your tree):
PYTHONPATH="$(pwd)" ~/.virtualenvs/ray-apiref/bin/python \
  ci/ray_ci/doc/cmd_check_api_discrepancy.py "$(pwd)" serve
```

Pass a single team (`core`, `data`, `serve`, `train`, `tune`, `rllib`) or omit the argument to check all of them.

```{warning}
`setup-dev.py` links your checkout's Python packages over the wheel, so the walked `@PublicAPI` surface reflects your local changes. The rest of the wheel (compiled extensions and generated code) is still the nightly build, cut at a different commit than your checkout, so treat a clean local run as a fast iteration signal rather than a definitive result. For sign-off, rely on the CI run in the docbuild image.
```
