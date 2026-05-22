# Contributing to Ray Clone

We welcome contributions! This document outlines the rules and processes for contributing to this repository.

## OSS Contribution Rules

As a clone of the Ray project, we adhere to the general contribution guidelines of the upstream repository:

1. **Bug Reports**: Use GitHub Issues to report bugs. Provide a minimal reproduction script if possible.
2. **Feature Requests**: Open an issue to discuss new features before starting implementation.
3. **Pull Requests**: All changes should be submitted via Pull Requests.

## Local Development Rules

### Linting and Formatting

We use several tools to ensure code quality. Please run them before submitting a PR:

- **Python**: We use `pylint` and `black`/`yapf` (check project config).
- **C++**: We use `clang-format`.
- **Pre-commit Hooks**: It is highly recommended to install the pre-commit hooks:
  ```bash
  ./setup_hooks.sh
  ```

### Testing

Always add tests for new features or bug fixes.

- **Python Tests**:
  ```bash
  pytest python/ray/tests/test_your_feature.py
  ```
- **Bazel Tests (Core/C++/Java)**:
  ```bash
  bazel test //src/...
  bazel test //java/...
  ```

## PR Review Process

1. **Assign a Reviewer**: When you create a PR, add a relevant maintainer as an assignee.
2. **Address Comments**: Reviewers will provide feedback. Use the `@author-action-required` label if you need to signal that you are working on changes.
3. **CI/CD**: Ensure all tests pass in the CI pipeline. The `test-ok` label indicates that the build is successful.
4. **Merging**: Maintainers will merge the PR once it is approved and the build passes.

## Style Guidelines

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) for Python.
- Follow the [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) for C++.
- Use clear and descriptive commit messages.
