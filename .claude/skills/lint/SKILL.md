---
name: lint
description: Run linting and formatting checks on Ray code
---

# Lint Ray Code

Refer to doc/source/ray-contribute/development.rst "Development tooling" section
for current tool versions and setup. update the skill if any changes are detected.

## Python linting

```bash
# Format
ruff format <paths>

# Check
ruff check <paths>

# Fix auto-fixable issues
ruff check --fix <paths>
```

## C++ formatting

```bash
./ci/lint/check-format.sh --fix
```

## Pre-commit (all checks)

```bash
pre-commit run --all-files      # All files
pre-commit run                  # Staged files only
```

## Notes
- If pre-commit is not installed: `pip install -c python/requirements_compiled.txt pre-commit && pre-commit install`
- Configuration: .pre-commit-config.yaml, pyproject.toml (ruff)
