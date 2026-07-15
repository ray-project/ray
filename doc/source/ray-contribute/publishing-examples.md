---
myst:
  html_meta:
    description: "How to publish an executable example to the Ray documentation: the recommended template-collections flow and the legacy in-tree notebook flow, plus how to register a release test so CI keeps your example working."
---

(publishing-examples)=

# Publishing an example with CI

This page describes how to publish an executable example to the Ray documentation and wire it into continuous integration so it keeps working as Ray changes. It covers the recommended template-collections flow, the legacy in-tree notebook flow, and the checks every example passes before merging.

Before you start, read {ref}`contributing to the Ray documentation <docs-contribute>` for how to build and preview the docs locally, and {ref}`the documentation style guide <documentation-style>`.

## Two ways to publish an example

An example reaches [docs.ray.io](https://docs.ray.io) through one of two paths:

- **The template-collections flow (recommended).** You author the example as an Anyscale template. The Ray docs build fetches it at build time and renders it in place. Templates are version-tested and automatically kept current, so your example stays working without a per-example release test to maintain.
- **The in-tree notebook flow (legacy).** You commit a notebook directly under `doc/source`, wire its table of contents by hand, and register a release test that reruns it on a schedule. This path is being phased out. Only a couple of examples still use it, and you keep every part of it working yourself.

Prefer the template-collections flow for new examples. Use the in-tree notebook flow only for an example that can't be a template, and expect to maintain its release test.

## Publish with the template-collections flow

In this flow the example's content lives in a template, not in the Ray repository. The Ray docs build pulls the template's files at build time using [sphinx-collections](https://sphinx-collections.readthedocs.io/), then renders them under a `_collections/` path as if they were in the tree. You register the example in Ray docs in three small edits.

### 1. Author and publish the template

Author your example as an Anyscale template so it's both a workspace people can launch and a docs page. A template ships a `README.md` and a matching `README.ipynb`. The docs build renders the `README.md` as the example page and excludes the duplicate notebook. If you're not sure whether your example should be a template, ask the Ray docs team.

### 2. Register the template in the docs build

Add an entry to the `_TEMPLATE_COLLECTIONS` dictionary in [`doc/source/template_collections.py`](https://github.com/ray-project/ray/blob/master/doc/source/template_collections.py). The key is the template's name. `target` is the `_collections/` path the build renders it under:

```python
_TEMPLATE_COLLECTIONS = {
    # ...
    "my-example-template": {
        "target": "ray-overview/examples/my-example",
    },
}
```

The build fetches each template's files into `_collections/<target>/` and renders `README.md` there.

### 3. Pin the template build

The docs build fetches an exact, pinned build of each template rather than its latest build, so a docs build is reproducible. Add your template to [`doc/source/template_pins.json`](https://github.com/ray-project/ray/blob/master/doc/source/template_pins.json). A template with no pin still builds by falling back to its latest build, but it logs a warning, so add the pin. Pins are bumped automatically by a workflow that tracks each template's latest build, so you don't hand-maintain the pin after adding it.

### 4. Register the example in the gallery

Add an entry to the `examples.yml` for the relevant library so the example appears in that library's examples gallery. Point `link` at the rendered `_collections/` path, without a file extension:

```yaml
- title: My example title
  skill_level: beginner
  frameworks:
    - pytorch
  use_cases:
    - computer vision
  link: ../_collections/ray-overview/examples/my-example/README
```

For the exact fields each library's gallery accepts, follow the existing entries in that `examples.yml`. See [`doc/source/train/examples.yml`](https://github.com/ray-project/ray/blob/master/doc/source/train/examples.yml) for entries that link to templates this way.

## Publish with the in-tree notebook flow (legacy)

Use this flow only when an example can't be a template. It commits the notebook to the Ray repository and registers a release test that reruns it on a schedule against Ray's nightly build.

### Set up your working directory

Fork and clone Ray, then branch from `upstream/master`. Under the location for your library (for example, `doc/source/ray-overview/examples` for general examples or `doc/source/serve/tutorials` for Ray Serve), create a directory for your example with two subdirectories:

```text
doc/source/<library>/examples/my_example/
├── ci/         # files CI needs to run your test
└── content/    # everything rendered in the docs
```

Use underscores, not dashes, in the directory name. The release test name is regex-sensitive, and reusing the same name for the directory and the test keeps them easy to maintain.

### Add your content and wire the table of contents

Put your notebook or Markdown under `content/`. Sphinx discovers `.ipynb`, `.md`, and `.rst` files automatically, but you still register the page in a table of contents. How you do that depends on the library:

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

For Ray Train examples, the `toctree` warns about a page it can't reach from a parent. Add `:orphan:` metadata to a Train example page that the gallery links to through `examples.yml` rather than through a `toctree`.

### Add the CI test

In `ci/`, add a `tests.sh` entrypoint that runs your example, and compute configs for both clouds, `aws.yaml` and `gce.yaml`. CI injects the cloud and region, so leave those as placeholders. Run commands from the project root, not from `ci/`.

Notebooks can hide a failure when a converter runs cells in a subprocess. Convert the notebook to a runnable module and run that instead so failures surface. Existing examples use a small `nb2py.py` helper for this. Set strict flags in the script:

```bash
#!/usr/bin/env bash
set -euxo pipefail

python ci/nb2py.py content/my_notebook.ipynb --out /tmp/my_notebook.py
python /tmp/my_notebook.py
```

Make the script executable with `chmod +x ci/tests.sh`. For an example that reads a secret such as an API key, don't put the secret in the build. Ask the Ray docs team to add it to the secret manager and read it at runtime in your test script.

### Register the release test

Add an entry for your example in [`release/release_tests.yaml`](https://github.com/ray-project/ray/blob/master/release/release_tests.yaml) so CI runs it on a schedule:

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

### Trigger the release test

CI runs your test on the schedule you set, but the `buildkite/release` check on a pull request is paused by default and appears green without running. Trigger it manually to verify your example, and retrigger it on every new commit:

1. In the pull request's checks, open **buildkite/release** and choose **Rebuild**.
2. Choose **Specify which release tests you want to run**.
3. In the test filter, enter `name:my_example\.aws`, using your test's name.

A full run takes about 60 to 90 minutes. Inspect the linked job's logs to confirm it ran, because some failures don't mark the job as failed.

## Checks required before merge

Both flows pass the same checks before a pull request merges:

- **Local build.** Build the docs locally to catch Sphinx errors early. See {ref}`building the Ray documentation <build-ray-docs>`.
- **Vale.** Vale lints Markdown, not notebooks. Convert your notebook to Markdown with `jupyter nbconvert example.ipynb --to markdown`, run `vale` on the output, fix the findings in the notebook, and reconvert. See {ref}`how to use Vale <vale>`.
- **Sign-off.** Every commit needs a Developer Certificate of Origin sign-off. Commit with `git commit -s`.
- **Pre-merge tests.** Add the `go` label to run the full pre-merge suite. Without it, only the lighter microcheck runs. Ask the Ray docs team to apply the label. Most failures are formatting or linting, which the microcheck surfaces too.

Your pull request is ready to merge once reviewers approve it, it carries the `go` label, all checks pass, and you've triggered `buildkite/release` on your latest commit. After it merges, the change appears on docs.ray.io within a few hours on the `master` version and in `latest` after the next Ray release.
