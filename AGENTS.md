# Agent Instructions for Ray

> These instructions apply to **all** AI-assisted contributions to `ray-project/ray`.
> PRs that ignore this policy may be closed without review.

Ray is a high-traffic repository. Every PR notifies CODEOWNERS, triggers CI, and
consumes maintainer attention. Automated, low-value, or duplicate contributions
create real cost for the people who maintain the project. Follow the rules below
before opening any PR with AI assistance.

## 1. Contribution Policy (Mandatory)

### Duplicate-work checks

Before proposing a PR, confirm the work is not already in flight:

```bash
# If you are addressing an existing issue:
gh issue view <issue_number> --repo ray-project/ray --comments
gh pr list --repo ray-project/ray --state open --search "<issue_number> in:body"

# Search open PRs for the same area before starting:
gh pr list --repo ray-project/ray --state open --search "<short area keywords>"
```

- If an open PR already addresses the same change, do not open another. Comment
  on the existing PR instead.
- If your approach is materially different, explain the difference in the issue
  or the existing PR thread before opening a competing PR.

### No low-value busywork PRs

Do not open one-off PRs for trivial edits (a single typo, an isolated style
tweak, one mutable default, a lone type annotation, etc.). Mechanical cleanups
are acceptable only when bundled with substantive work, or when coordinated with
maintainers first. Mass-generated "cleanup" PRs are not welcome.

### Accountability

- Pure code-agent PRs are **not allowed**. A human submitter must understand and
  defend the change end-to-end.
- The submitting human must review every changed line and run the relevant tests
  locally before requesting review.
- PR descriptions for AI-assisted work **must** state:
    - Why this is not duplicating an existing issue or PR.
    - The test commands run and their results.
    - That AI assistance was used.

### Fail-closed behavior

If the requested work is a duplicate, trivial busywork, or cannot be tested and
defended by a human, **do not open a PR**. Return a short explanation of what is
missing instead.

## 2. Development Workflow

Ray is a unified framework for scaling AI and Python applications. Its source is
laid out as:

- `src/ray/`: C++ core runtime
- `python/ray/`: Python API and libraries (data, rllib, serve, train, tune)
- `doc/source/`: Sphinx documentation

The default test timeout is 180s (`pytest.ini`). For build, test, lint, and
code-style details, follow Ray's existing guides rather than duplicating them
here:

- Setting up a dev environment and code style:
  [`doc/source/ray-contribute/getting-involved.md`](doc/source/ray-contribute/getting-involved.md)
- Building Ray:
  [`doc/source/ray-contribute/development.md`](doc/source/ray-contribute/development.md)
- General contribution process: [`CONTRIBUTING.md`](CONTRIBUTING.md)

### Changes to public API behavior

A change to a `@PublicAPI` or `@DeveloperAPI` symbol under `python/ray/` is
not complete until its documentation matches the new behavior. In the same PR:

- **Docstrings.** Update the docstring of every symbol whose signature,
  defaults, return value, raised exceptions or behavior changed. Each
  parameter needs an entry in the `Args:` block. For `@PublicAPI` symbols,
  CI's `api_param_coverage` check fails on a new parameter without one; it
  doesn't check `@DeveloperAPI` symbols. Keep `>>>` examples runnable and
  their output accurate.
- **API reference.** Add a new `@PublicAPI` symbol to its library's API
  reference page, and remove a deleted one. `API_PATH_PREFIXES` in
  `doc/source/_ext/api_sidebar.py` lists the API reference directories. CI's
  `doc: check API doc consistency` step fails when the documented and
  annotated `@PublicAPI` surfaces disagree. Removing or renaming a symbol also
  changes its reference page URL; see the next section.
- **User guides and examples.** Search `doc/source/` for the symbol's name
  and its import path, including `.py` snippets under `doc_code/` and `.ipynb`
  notebooks, and update any prose, code snippet or example that now describes
  the old behavior or uses a removed or renamed argument. Before editing
  anything under `doc/`, read `doc/AGENTS.md`.
- **Deprecations.** When deprecating an API, mark it with
  `@Deprecated(message=...)` from `ray.util.annotations`, name the replacement
  in the message, and update docs examples to use the replacement.

Internal refactors that leave user-visible behavior and public import paths
unchanged need no docs update.

### Changes that move docs.ray.io URLs

Renaming, moving, or removing a public API under `python/ray/` changes or
removes the URL of its generated API reference page, even when the PR touches
nothing under `doc/`. So does moving or renaming a page under `doc/source/`. In
the same PR, add a redirect in `doc/redirects/current.yaml` from each old path to
its new path, or for a removed API to its successor's path, and run
`rtd-redirects validate doc/redirects/current.yaml` before pushing.
[`doc/redirects/README.md`](doc/redirects/README.md) covers which rule to write
and how to install the tool, which needs no credentials.

### Required for every commit

- **DCO sign-off.** All commits require a Developer Certificate of Origin
  sign-off. Always commit with `-s`:

  ```bash
  git commit -s -m "Your commit message"
  ```

- **Pre-commit hooks.** Install pre-commit. With `pre-commit install`, the hooks
  run automatically on staged files at commit time:

  ```bash
  pip install -U pre-commit==3.5.0 && pre-commit install
  pre-commit run
  ```

- **Python environments.** Use a virtual environment for all Python work. Never
  install into system Python.

## 3. Editing these instructions

Changes to this file affect every AI-assisted contribution to Ray. Open a
dedicated PR for any change to `AGENTS.md`, explain the motivation, and tag the
maintainers who own contribution policy for review.
