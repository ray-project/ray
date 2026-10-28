# `ci/ray_ci/doc`

CI code for building the Sphinx documentation site, guarding the API reference
against drift from the annotated Python surface, and running the doc example
and docstring tests that the `docs-example-test` and `docstring-only` PR labels
opt into. Everything here runs from Buildkite; none of it is imported by Ray at
runtime.

The build and API-guard entry points are the `cmd_*.py` modules in the table
below. The label-driven scripts are described in
[Label-driven test selection](#label-driven-test-selection).

## Entry points

| Module | Invoked by | What it does |
| --- | --- | --- |
| `cmd_build.py` | `bazel run //ci/ray_ci/doc:cmd_build`, from the `doc: build` step in `.buildkite/doc.rayci.yml` | Runs `make html` in `doc/`, then uploads the build artifacts to S3 as a cache for later builds. |
| `cmd_check_api_discrepancy.py` | `ci/lint/lint.sh api_policy_check`, from the `doc: check API doc consistency` step | Compares the `@PublicAPI` surface reached by importing Ray against the API surface the reference pages document. Reports both directions of mismatch, plus APIs documented in more than one place and annotated subpackages the walk never reaches. |
| `cmd_check_api_param_coverage.py` | `ci/lint/lint.sh api_param_coverage`, from `.buildkite/lint.rayci.yml` | Fails a PR that adds a new `@PublicAPI` callable, or a new parameter on an existing one, without a docstring `Args:` entry. |

Two of the three don't run through Bazel in CI even though they have
`py_binary` targets. `lint.sh` invokes them with the docbuild image's
interpreter because the Bazel targets resolve `@py_deps_py310` wheels, which
can't import under the py3.11 docbuild image. `cmd_build` is the exception: it
does run as `bazel run`.

The `doc: check API annotations` step is a different check that lives outside
this package, in `ci/lint/check_api_annotations.py`.

`linkcheck_report.py` runs after `make -C doc linkcheck_all` in the
postmerge-only, soft-fail `doc: linkcheck` step. It re-checks the links Sphinx
reported broken and writes the confirmed ones to a `linkcheck-report.json`
build artifact. It never fails the build.

## Label-driven test selection

These scripts back two PR labels. Both labels need write access to apply. See
`doc/source/ray-contribute/ci.md` for the contributor-facing behavior. Which
steps each label keeps or skips is set in `.buildkite/docs.labels.yaml`, which
rayci applies to PR builds before it marks step dependencies.

- **`docs-example-test`.** The `doc: docs example tests (opt-in)` step in
  `.buildkite/others.rayci.yml` runs `run_doc_example_tests.sh`. It selects
  targets with `cmd_doc_example_targets.py`, backed by `doc_example_targets.py`,
  which maps changed doc files to the Bazel test targets that name them
  directly, and never falls back to a library's whole docs example suite. Run
  the selector locally with
  `bazel run //ci/ray_ci/doc:cmd_doc_example_targets -- <changed files>`.
- **`docstring-only`.** `check_docstring_only.py` is the scope guard, run as
  `ci/lint/lint.sh docstring_only_scope` from the `lint: validate docstring-only
  scope` step in `.buildkite/lint.rayci.yml`. It fails unless every changed file
  is a modified `.py` file whose `ast` is unchanged once docstrings are removed.
  The `doc: docstring examples` step in `.buildkite/others.rayci.yml` runs
  `run_docstring_examples.sh`, which keeps the changed modules under
  `python/ray` that a CPU `doctest()` target covers and runs
  `run_docstring_examples.py` on them in the `docstringbuild` image.

## Libraries

**The API-reference guard.** `cmd_check_api_discrepancy.py` builds two sets of
API names and diffs them.

- `api.py` defines the `API` record and the annotation predicate. Its
  `_is_directly_annotated` deliberately matches `ray.util.annotations._is_annotated`,
  so the checker agrees with the runtime about what counts as annotated,
  including on the edge cases. It also parses the Sphinx `autoclass` and
  `autosummary` directive forms into `API` records.
- `module.py` walks a top-level module depth-first at runtime and collects
  every directly annotated class and function it reaches. This is the
  code-side set. It also tracks which modules the walk actually reached, which
  is what surfaces a module annotated but never imported by its parent's
  `__init__`.
- `autodoc.py` walks the reference pages instead: starting from a landing RST
  file, it follows `include` and `toctree` references to find every autodoc RST,
  then parses the `autoclass` and `autosummary` blocks in each. This is the
  docs-side set.

Per-team scope lives in the `TEAM_API_CONFIGS` table in
`cmd_check_api_discrepancy.py`: which modules to walk, which landing RST to
read, and the exemption lists. That file's header explains what each exemption
list means and when an entry should move between them.

**Parameter coverage.** `api_param_coverage.py` backs
`cmd_check_api_param_coverage.py`. It's static and diff-scoped: it parses the
base-branch and working-tree versions of the changed files with `ast` and
compares their undocumented-parameter sets, so it needs no Ray build or import
environment. Pre-existing undocumented parameters are grandfathered; only new
ones on the changed public surface are reported.

**Build cache.** `build_cache.py` collects the untracked files `make html`
produced, tars them, and uploads to S3. It uploads only on postmerge builds of
`master`; anywhere else it runs as a dry run.

`massage_cache.py` is a separate script rather than a function because of an
interpreter split. Before the artifacts can serve as a cache, the Sphinx
environment pickle has to be stripped of `site-packages` paths, which are local
to the build machine and would otherwise mark every doc that imports them as
outdated when the cache is restored elsewhere. A pickled Sphinx environment can
only be unpickled by a matching Sphinx, so the strip has to run under the
interpreter that built the docs, not this package's Bazel-pinned one.
`BuildCache._massage_cache` shells out to it with `PYTHONPATH` unset, and the
script imports only the standard library so any interpreter can run it.

## Tests

The `test_*.py` files run as `ci_unit` Bazel targets tagged `team:ci`. Several
of them depend on the fixture package in `mock/` rather than on real Ray.

```bash
bazel test //ci/ray_ci/doc/...
```
