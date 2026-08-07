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
  run other serve docs example tests

  @microcheck //doc:source/serve/doc_code/distilbert //doc:source/serve/doc_code/object_detection //doc:source/serve/doc_code/stable_diffusion

  Signed-off-by: can <can@anyscale.com>
  ```

If microcheck passes, you'll see a green checkmark on your PR. If it fails, you'll see a red cross. In either case, you'll see a summary of the test run statuses in the GitHub UI.

## Additional tests at merge time

`microcheck` runs on every commit, but the full test suite must pass before a PR can merge. Adding the `go` label triggers the full suite, and committers require the `go` tests to have passed before adding a PR to the merge queue.

If you're a committer, add the `go` label to your PR once it's ready, then merge after the full suite passes. Clicking **Enable auto-merge** does both in one step: it adds the `go` label and merges the PR automatically once the suite passes. Pushing a new commit disables auto-merge, so re-enable it afterward. When you review an external contributor's PR, add the `go` label for them, since they can't add it themselves.

If you're an external contributor, adding the `go` label and enabling auto-merge both require write access, so a committer runs the full suite and merges when your PR is ready.

## Documentation CI checks

A change under `doc/` runs a different set of checks than a code change, and which ones run depends on the kind of file you touched. Ray routes documentation changes by path, so a change to one library's docs doesn't run every other library's tests.

### The Read the Docs render gate

Read the Docs builds the site on your PR and reports the `docs/readthedocs.com:anyscale-ray` check. The build runs `make -C doc html` with `fail_on_warning: true`, so any Sphinx warning fails it. A malformed directive, a broken cross-reference, or a page missing from a toctree surfaces here rather than in Buildkite.

The preview build skips when your PR changes nothing under `doc/` and nothing in `.readthedocs.yaml`. Files under `doc/.claude/` don't count, because they never participate in the Sphinx build. This check isn't a required merge gate, but a red render gate almost always means the published page is broken.

### The API surface checks

Two Buildkite steps cross-check the documented API surface against the code:

* `doc: check API annotations` verifies the `@PublicAPI`, `@DeveloperAPI`, and `@Deprecated` annotations in the source.
* `doc: check API doc consistency` verifies that the annotated surface and the surface documented in the API reference pages agree.

Both run when you change library code, and also when you change an API reference page such as `doc/source/data/api/` or the autodoc machinery under `doc/source/_ext/`. Those paths carry a dedicated `doc_api` tag so an API page edit still reaches the checks, even though it's a documentation change. To reproduce the second check on your own machine, see [Running the API consistency check locally](#running-the-api-consistency-check-locally).

### Per-library docs example tests

Each library runs the executable examples in its own documentation in a step named `<library>: docs example tests`. These execute the `doctest`, `testcode`, and `literalinclude` snippets described in [How to write code snippets](writing-code-snippets.md).

The steps are path-scoped, so editing one library's docs runs only that library's examples:

| Path you change | Step that runs | Tag |
| --- | --- | --- |
| `doc/source/ray-core/`, `doc/source/ray-observability/` | `core: docs example tests` | `core_doc` |
| `doc/source/data/`, `doc/source/ray-more-libs/` | `data: docs example tests`, `data: dask docs example tests` | `data_doc` |
| `doc/source/train/`, `doc/source/tune/`, `doc/source/ray-air/` | `ml: docs example tests` | `ml_doc` |
| `doc/source/rllib/` | `rllib: docs example tests` | `rllib_doc` |
| `doc/source/serve/` | `serve: docs example tests` | `serve_doc` |

Ray Core owns the fallback: an executable doc asset that doesn't sit under one of these directories routes to `core: docs example tests`. Ray LLM is the exception to the pattern, because `doc/source/llm/` routes to the general `llm` tag rather than a dedicated docs example step.

The `doc` tag itself no longer selects these steps. It now covers the documentation build and validation infrastructure, such as `.readthedocs.yaml` and the docs dependency locks.

### What a prose-only change runs

Narrative documentation and images don't block premerge. A PR that changes only `.md`, `.rst`, or image files under `doc/` runs no library test steps at all. Neither can change tested code, and the post-merge documentation build is the coverage for them. Executable assets such as `.py` and `.ipynb` still route to the owning library, and so do config assets that examples consume, such as `.yaml` and `.sh`, because those can change what a test does.

A prose-only change also skips most of the lint group. Five lint steps run: a README check, a ban on newly added `.rst` files, since new pages must be MyST Markdown, a documentation style linter, a banned-words check, and a small pre-commit step covering the three hooks that reach prose (trailing whitespace, end-of-file newline, and added large files).

The code-oriented lint steps don't run, because a Markdown change can't fail them: `clang_format`, `bazel_buildifier`, `bazel_team`, `copyright_format`, `dashboard_format`, `pytest_format`, `test_coverage`, `semgrep_lint`, the full 23-hook `pre_commit`, and pydoclint. This matters more than it sounds: those steps each pay a full repository clone, and the full `pre_commit` runs every hook with `--all-files`, so its cost is the same on a one-line Markdown edit as on a thousand-file C++ change.

The split lives in `.buildkite/always.rules.txt`, which emits the `lint` tag for every file except prose and images. One non-prose file anywhere in your diff brings the whole lint group back, so a mixed PR loses no coverage. Post-merge builds run everything regardless, since rayci skips rule evaluation outside pull requests.

### Skipping example tests with the `docs-go` label

Adding the `docs-go` label skips the per-library docs example steps. It's optional. Reach for it on a content-only PR where executing the examples adds nothing and you don't want to wait on them. Like the `go` label, it requires write access, so an external contributor asks a reviewer to add it.

A guard step, `lint: validate docs-go scope`, bounds what the label can skip. It runs whenever the label is present and fails unless the PR changes only content under `doc/`. So the label skips example tests on a prose change but never on a code, CI, or build change. The API surface checks ignore the label by design, since an API reference page edit is exactly the content-only change they need to cover.

### What doesn't run on your PR

Two heavier steps are post-merge only, and both carry `skip-on-premerge`:

* `doc: build` renders the site with `make html` and uploads the build artifacts to S3, which it does only on `master`.
* `doc: linkcheck` validates links and is soft-fail, so it reports without blocking.

For building and previewing the docs on your own machine, see [Contributing to the Ray documentation](docs.md).

## Running the API consistency check locally

The `doc: check API doc consistency` premerge step runs the `api_policy_check` function in `ci/lint/lint.sh`, which does three things: installs Ray from your checkout with `--no-deps`, generates the autosummary stub `.rst` files with `doc/source/api_autogen.py`, then runs `ci/ray_ci/doc/cmd_check_api_discrepancy.py`. The checker compares each team's documented API surface against the annotated (`@PublicAPI`/`@DeveloperAPI`/`@Deprecated`) surface in the code, importing Ray's own symbols for real. So it needs an environment where Ray installs and imports, which is the opposite of the docs *build* environment, which deliberately doesn't install Ray (see [Building the Ray documentation](docs.md#building-the-ray-documentation)).

The optional third-party backends that some surfaces pull in at import time don't have to be installed. Examples include the vLLM and SGLang stack behind `ray.data.llm` and `ray.serve.llm`, and the deep-learning backends behind `ray.train`, `ray.tune`, and `ray.rllib`. The check mocks whichever of them are absent, reading the list from `doc/source/api_mock_imports.py`, the same source of truth the docs build uses for `autodoc_mock_imports`. It mocks only the absent ones, because shadowing an installed library breaks the plain `import` the walk relies on.

### The faithful environment: the docbuild image

CI runs this check inside the `docbuild` image (`ci/docker/doc.build.Dockerfile`). The image doesn't pip-install Ray. It stages the prebuilt `ray-core` and `ray-dashboard` artifacts into `/opt/ray-build` on top of the `oss-ci-base_build` base image, then installs the docs dependency lock (`python/deplocks/docs/docbuild_depset_py3.11.lock`), which is where Sphinx and Jinja2 come from. At step time `lint.sh` unpacks those artifacts and runs `pip install -e "python[all]" --no-deps`, so the walked surface is your checkout's source over prebuilt compiled extensions.

The base image supplies `python/requirements.txt`, so a few of the libraries the walk imports are real rather than mocked, `pandas` most notably. The heavy deep-learning backends aren't installed even in CI: `torch`, `transformers`, and `tensorflow` are all mocked there too. Because the lock pins Linux wheels, the only fully faithful local reproduction is the Linux docbuild image through Docker, or a CI run on your branch.

### A lightweight local approximation

For quick iteration on the checker itself, a Python 3.11 virtual environment with a nightly Ray wheel and Sphinx is usually enough. Sphinx isn't a Ray dependency, and both halves of the step need it: the checker imports `sphinx.ext.autodoc.mock` and the stub generator imports `sphinx.ext.autosummary`. You generally don't need to install the mocked backends by hand. Install one for real only when you want the check to walk that library's true surface instead of a mock.

```bash
uv venv --python 3.11 ~/.virtualenvs/ray-apiref
source ~/.virtualenvs/ray-apiref/bin/activate

# The wheel URL below is macOS arm64 / Python 3.11. Adjust it for your OS, architecture, and Python version.
uv pip install sphinx \
  "https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp311-cp311-macosx_11_0_arm64.whl"

# Link your checkout's Python files over the wheel so the check walks your local @PublicAPI changes, not the wheel's:
python python/ray/setup-dev.py --yes

# Generate the autosummary stubs the check reads, as CI does in the same step:
PYTHONPATH="$(pwd)" python doc/source/api_autogen.py

# Run the check against your checkout (PYTHONPATH so the checker imports from your tree):
PYTHONPATH="$(pwd)" python ci/ray_ci/doc/cmd_check_api_discrepancy.py "$(pwd)" serve
```

Pass a single team (`core`, `data`, `serve`, `train`, `tune`, `rllib`) to check one surface, or pass `ALL` or omit the argument to check every team. These team names don't line up with the steps in [Per-library docs example tests](#per-library-docs-example-tests), so two cases need translating: the `ml` step covers `train` and `tune`, which the check treats as separate teams, and there's no `llm` team at all, since the check walks `ray.data.llm` under `data` and `ray.serve.llm` under `serve`. Two things only happen on the full pass. Every team after `core` depends on `core` running first, and the cross-team guard that catches annotated public subpackages no team's walk reaches runs at the end. Run without a team argument before you trust a green result.

A bare Ray wheel doesn't pull in `pandas`, so locally the check mocks it where CI walks it for real. Install `pandas` in the virtual environment when you're checking the `data` team.

```{warning}
`setup-dev.py` links your checkout's Python packages over the wheel, so the walked `@PublicAPI` surface reflects your local changes. The rest of the wheel (compiled extensions and generated code) is still the nightly build, cut at a different commit than your checkout, so treat a clean local run as a fast iteration signal rather than a definitive result. For sign-off, rely on the CI run in the docbuild image.
```
