---
name: lint
description: Run linting and formatting checks on Ray code
---

# Lint Modified Files

Run linting and formatting only on files you changed. Use `git diff --name-only` to
get the list of modified files, then pass them directly to the linters.

## Python

```bash
ruff check --fix <changed .py files>
ruff format <changed .py files>
```

## C++

```bash
./ci/lint/check-format.sh --fix
```

## Handling remaining errors

After running `ruff check --fix`, review any remaining errors that could not be
auto-fixed. These typically require code changes — for example, adding a missing
import, resolving a name conflict, or restructuring logic.

Fix these by editing the source code directly. Use `# noqa` only for false positives
that cannot be resolved by changing the code, and include the rule code and reason:
`# noqa: E501 — URL cannot be split`.

Entries in pyproject.toml `per-file-ignores` and `extend-exclude` are managed by the
team. Use them as-is; avoid adding new exclusions.

## Reference

- Config: pyproject.toml (`[tool.ruff]` section)
- Docs: doc/source/ray-contribute/development.rst ("Development tooling")
