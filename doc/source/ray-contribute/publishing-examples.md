---
myst:
  html_meta:
    description: "How to publish an executable example to the Ray documentation with the template-collections flow: author a template, register it in the docs build, pin its build, and add it to the examples gallery."
---

(publishing-examples)=

# Publishing an example

This page describes how to publish an executable example to the Ray documentation with the template-collections flow. To add a guide that Ray CI doesn't test, without publishing it as a template, see {ref}`writing-e2e-guide`.

## How the template-collections flow works

You author the example as an Anyscale template. The Ray docs build fetches it at build time and renders it in place. Templates are version-tested and automatically kept current, so your example stays working without a per-example release test to maintain.

Don't publish a new example as an in-tree notebook. Two existing examples still use that legacy flow, which the Ray documentation is retiring. To maintain one of them, see {ref}`publishing-examples-legacy`.

## Publish with the template-collections flow

In this flow the example's content lives in a template, not in the Ray repository. The Ray docs build pulls the template's files at build time using [sphinx-collections](https://sphinx-collections.readthedocs.io/), then renders them under a `_collections/` path as if they were in the tree. You register the example in Ray docs in the following edits.

### 1. Author and publish the template

Author your example as an Anyscale template so it's both a workspace people can launch and a docs page. See the [`anyscale/templates` repository](https://github.com/anyscale/templates) and its "Contributing a template" guide for how to author one. A template ships a `README.md` and a matching `README.ipynb`. The docs build renders the `README.md` as the example page and excludes the duplicate notebook. If you're not sure whether your example should be a template, ask the Ray docs team.

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

## Open your pull request

These edits are configuration changes to the Ray repository, so they go through the normal contribution checks:

- **Local build.** Build the docs locally to catch Sphinx errors early. See {ref}`building the Ray documentation <build-ray-docs>`.
- **Sign-off.** Every commit needs a Developer Certificate of Origin sign-off. Commit with `git commit -s`.
- **Pre-merge tests.** Add the `go` label to run the full pre-merge suite. Ask the Ray docs team to apply the label.

After the pull request merges, the example appears on docs.ray.io within a few hours on the `master` version, and in `latest` after the next Ray release.
