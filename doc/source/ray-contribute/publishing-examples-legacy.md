---
myst:
  html_meta:
    description: "How to maintain one of the in-tree notebook examples that remain in the Ray documentation: edit the notebook, control how its cells render, and run its CI and release tests. New examples use the template-collections flow."
---

(publishing-examples-legacy)=

# Maintaining an in-tree notebook example

This page describes how to maintain an example that's committed to the Ray repository as a notebook and validated by a release test. This in-tree notebook flow is legacy, and the Ray documentation is retiring it. Don't add new notebook examples. To publish a new example, use the template-collections flow. See {ref}`publishing-examples`. For a guide that Ray CI doesn't test, see {ref}`writing-e2e-guide`.

Two examples still use this flow:

- `doc/source/serve/tutorials/video-analysis`
- `doc/source/train/examples/transformers/transformer-reinforcement-learning`

:::{note}
You maintain and run every part of an in-tree example's release test yourself. The in-tree example release tests aren't run on a schedule, so treat the release test as an on-demand check you trigger manually.
:::

## Edit the notebook

Each example's directory holds a `README.ipynb` notebook, a `README.md` copy of it, and a `ci/` directory for its tests. `exclude_patterns` in `doc/source/conf.py` decides which of the two files the docs build renders, so when you change one, update the other to match.

If you add IPython syntax such as `!pip install` to a notebook, set its lexer so the build can parse it. In the notebook's `metadata.language_info`, set `pygments_lexer` to `ipython3`. Otherwise the default `python3` lexer fails the build on those cells.

## Hide and remove cells

When the docs build renders the notebook, you control how a cell renders with tags in the cell's `metadata.tags` list in the `.ipynb` file:

- `hide-cell`, `hide-input`, and `hide-output` collapse the whole cell, its input, or its output. Readers click to expand it.
- `remove-cell`, `remove-input`, and `remove-output` drop the cell, its input, or its output from the rendered page. The cell still runs when the notebook runs.

Removed cells keep compute-intensive examples fast to test. Show the realistic values you want readers to use in a visible cell:

```python
num_workers = 8
num_gpus = 2
```

Then follow it with a cell tagged `remove-cell` that overrides them with small values, so the test runs quickly:

```python
num_workers = 0
num_gpus = 0
```

## Update the CI test

The example's `ci/` directory holds a `tests.sh` entrypoint that runs the example, compute configs for both clouds, `aws.yaml` and `gce.yaml`, and an `nb2py.py` helper. CI injects the cloud and region, so leave those as placeholders in the compute configs. The release test runs `tests.sh` from the example's directory, not from `ci/`.

Notebooks can hide a failure when a converter runs cells in a subprocess. So `tests.sh` converts the notebook to a script with `nb2py.py` and runs that script, which surfaces failures. Keep strict flags in the script:

```bash
#!/usr/bin/env bash
set -euxo pipefail

python ci/nb2py.py README.ipynb README.py
python README.py
rm README.py
```

For an example that reads a secret such as an API key, don't commit the secret to the repository. Ask the Ray docs team to add it to the secret manager and read it at runtime in your test script.

## Update the release test

Each example has an entry under "Ray examples tests" in [`release/release_tests.yaml`](https://github.com/ray-project/ray/blob/master/release/release_tests.yaml). The entry's `working_dir` points at the example's directory, and its `run.script` runs `ci/tests.sh`. You trigger the test by its `name` with a regular-expression filter, so don't use dashes in the name.

If the example needs a different image or Python version, choose a `byod.type` from `ALLOWED_BYOD_TYPES` in [`release/ray_release/config.py`](https://github.com/ray-project/ray/blob/master/release/ray_release/config.py) that matches. The entry's `post_build_script` lives under [`release/ray_release/byod/`](https://github.com/ray-project/ray/tree/master/release/ray_release/byod) and installs anything the test needs beyond the base image.

## Trigger the release test

The `buildkite/release` check on a pull request is paused by default and appears green without running, and the in-tree example release tests aren't relied on for scheduled runs. Trigger the test manually to validate your example, and retrigger it on every new commit:

1. In the pull request's checks, open **buildkite/release** and choose **Rebuild**.
2. Choose **Specify which release tests you want to run**.
3. In the test filter, enter the test's name followed by `\.aws`, such as `name:video_analysis_pipeline\.aws`.

A full run takes about 60 to 90 minutes. Inspect the linked job's logs to confirm it ran, because some failures don't mark the job as failed.

## Before you merge

A change to an in-tree example passes the same checks as any documentation pull request, plus one notebook-specific step:

- **Vale.** Vale lints Markdown, not notebooks. Run `vale` on the example's `README.md`, and make the same fixes in the notebook. See {ref}`how to use Vale <vale>`.
- **Local build.** Build the docs locally to catch Sphinx errors early. See {ref}`building the Ray documentation <build-ray-docs>`.
- **Sign-off.** Every commit needs a Developer Certificate of Origin sign-off. Commit with `git commit -s`.
- **Pre-merge tests.** Add the `go` label to run the full pre-merge suite. Ask the Ray docs team to apply the label.

Your pull request is ready to merge once reviewers approve it, it carries the `go` label, all checks pass, and you've triggered `buildkite/release` on your latest commit. After it merges, your changes appear on docs.ray.io within a few hours on the `master` version, and in `latest` after the next Ray release.
