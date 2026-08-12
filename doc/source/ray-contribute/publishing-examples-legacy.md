---
myst:
  html_meta:
    description: "The legacy in-tree notebook flow for publishing an example to the Ray documentation: commit a notebook under doc/source, wire its table of contents, and register a release test that reruns it in CI. Prefer the template-collections flow for new examples."
---

(publishing-examples-legacy)=

# Publishing an example with the in-tree notebook flow (legacy)

This page describes the legacy flow for publishing an example: commit a notebook to the Ray repository and register a release test that you trigger manually to validate it in CI.

:::{note}
Prefer the template-collections flow for new examples. See {ref}`publishing-examples`. Only a couple of examples still use this flow, and you maintain and run every part of its release test yourself. The in-tree example release tests aren't run on a schedule, so treat the release test as an on-demand check you trigger manually. Use this flow only for an example that can't be a template.
:::

For how to write the notebook content itself, such as cells, tags, and local testing, see {ref}`creating-notebook-example`. This page picks up once you have a finished notebook, and covers placing it and wiring CI.

## Set up your working directory

Fork and clone Ray, then branch from `upstream/master`. Under the location for your library (for example, `doc/source/ray-overview/examples` for general examples or `doc/source/serve/tutorials` for Ray Serve), create a directory for your example with two subdirectories:

```text
doc/source/<library>/examples/my_example/
├── ci/         # files CI needs to run your test
└── content/    # everything rendered in the docs
```

Use underscores, not dashes, in the directory name. The release test name is regex-sensitive, and reusing the same name for the directory and the test keeps them easy to maintain.

## Place your example and wire the table of contents

Put your finished notebook under `content/`. Sphinx discovers `.ipynb`, `.md`, and `.rst` files automatically, but you still register the page in a table of contents. How you do that depends on the library:

:::{list-table}
:header-rows: 1

* - Library
  - Where to register the page
* - General examples
  - Add the page to the `toctree` in `doc/source/ray-overview/examples/index.rst`.
* - Ray Core
  - Add the page to the `toctree` in `doc/source/ray-core/examples/overview.rst` and add a row under the matching skill-level section.
* - Ray Serve, Ray Data, Ray Train
  - Add an entry to the library's `examples.yml`, which generates the gallery and its `toctree`.
* - Ray Tune
  - Add the page to the category `toctree` and the matching `list-table` in `doc/source/tune/examples/index.rst`.
:::

If your notebook uses IPython syntax such as `!pip install`, set its lexer so the build can parse it. In the notebook's `metadata.language_info`, set `pygments_lexer` to `ipython3`. Otherwise the default `python3` lexer fails the build on those cells.

A page that the gallery links to through `examples.yml` rather than through a `toctree` triggers a warning that it can't be reached from a parent. Mark it as an orphan to suppress the warning. In an `.ipynb`, add `"orphan": true` to the notebook's top-level `metadata`. In a MyST `.md` page, add `orphan: true` to the YAML front matter. This is common for Ray Train examples.

## Add the CI test

In `ci/`, add a `tests.sh` entrypoint that runs your example, and compute configs for both clouds, `aws.yaml` and `gce.yaml`. CI injects the cloud and region, so leave those as placeholders. Run commands from the project root, not from `ci/`.

Notebooks can hide a failure when a converter runs cells in a subprocess. Convert the notebook to a runnable module and run that instead so failures surface. Existing examples use a small `nb2py.py` helper for this. Set strict flags in the script:

```bash
#!/usr/bin/env bash
set -euxo pipefail

python ci/nb2py.py content/my_notebook.ipynb --out /tmp/my_notebook.py
python /tmp/my_notebook.py
```

Make the script executable with `chmod +x ci/tests.sh`. For an example that reads a secret such as an API key, don't commit the secret to the repository. Ask the Ray docs team to add it to the secret manager and read it at runtime in your test script.

## Register the release test

Add an entry for your example in [`release/release_tests.yaml`](https://github.com/ray-project/ray/blob/master/release/release_tests.yaml). This defines the test so you can trigger it manually in CI:

```yaml
- name: my_example            # no dashes (regex sensitive); used to trigger the test
  frequency: weekly
  python: "3.11"
  group: ray-examples
  team: serve                 # the team on call when the example breaks
  working_dir: //doc/source/ray-overview/examples/my_example

  cluster:
    byod:
      type: llm-cu130         # see ALLOWED_BYOD_TYPES in release/ray_release/config.py
      post_build_script: byod_my_example.sh
    cluster_compute: ci/aws.yaml

  run:
    timeout: 3600
    script: bash ci/tests.sh

  variations:
    - __suffix__: aws
    - __suffix__: gce
      env: gce
      frequency: manual
      cluster:
        cluster_compute: ci/gce.yaml
```

Choose a `type` from `ALLOWED_BYOD_TYPES` in [`release/ray_release/config.py`](https://github.com/ray-project/ray/blob/master/release/ray_release/config.py) that matches your image and Python version. Add a post-build script under [`release/ray_release/byod/`](https://github.com/ray-project/ray/tree/master/release/ray_release/byod) to install anything your test needs beyond the base image, and make it executable. If the base image is enough, the script can be a no-op.

You don't add a Bazel target for the compute configs. A glob in `doc/BUILD.bazel` discovers `ci/aws.yaml` and `ci/gce.yaml` under the standard example locations. If you add a new nested directory that has its own `BUILD.bazel`, add your configs to that directory's `*_examples_ci_configs` filegroup so the parent picks them up.

## Trigger the release test

The `buildkite/release` check on a pull request is paused by default and appears green without running, and the in-tree example release tests aren't relied on for scheduled runs. Trigger the test manually to validate your example, and retrigger it on every new commit:

1. In the pull request's checks, open **buildkite/release** and choose **Rebuild**.
2. Choose **Specify which release tests you want to run**.
3. In the test filter, enter `name:my_example\.aws`, using your test's name.

A full run takes about 60 to 90 minutes. Inspect the linked job's logs to confirm it ran, because some failures don't mark the job as failed.

## Before you merge

This flow passes the same checks as any documentation pull request, plus one notebook-specific step:

- **Vale.** Vale lints Markdown, not notebooks. Convert your notebook to Markdown with `jupyter nbconvert example.ipynb --to markdown`, run `vale` on the output, fix the findings in the notebook, and reconvert to Markdown to verify the fixes. See {ref}`how to use Vale <vale>`.
- **Local build.** Build the docs locally to catch Sphinx errors early. See {ref}`building the Ray documentation <build-ray-docs>`.
- **Sign-off.** Every commit needs a Developer Certificate of Origin sign-off. Commit with `git commit -s`.
- **Pre-merge tests.** Add the `go` label to run the full pre-merge suite. Ask the Ray docs team to apply the label.

Your pull request is ready to merge once reviewers approve it, it carries the `go` label, all checks pass, and you've triggered `buildkite/release` on your latest commit. After it merges, the example appears on docs.ray.io within a few hours on the `master` version, and in `latest` after the next Ray release.
