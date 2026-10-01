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
- `python/ray/`: Python API and libraries (data, serve, train, tune)
- `rllib/`: RLlib (symlinked from `python/ray/rllib`)
- `doc/source/`: Sphinx documentation

The default test timeout is 180s (`pytest.ini`). For build, test, lint, and
code-style details, follow Ray's existing guides rather than duplicating them
here:

- Setting up a dev environment and code style:
  [`doc/source/ray-contribute/getting-involved.md`](doc/source/ray-contribute/getting-involved.md)
- Building Ray:
  [`doc/source/ray-contribute/development.md`](doc/source/ray-contribute/development.md)
- General contribution process: [`CONTRIBUTING.rst`](CONTRIBUTING.rst)

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
