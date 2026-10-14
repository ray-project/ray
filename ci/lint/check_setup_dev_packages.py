#!/usr/bin/env python3
"""Check that the packages linked by ``python/ray/setup-dev.py`` still exist.

``setup-dev.py`` links the source tree into the installed Ray package for
Python-only development. When a package is renamed, moved, or removed, its
entry in the script's package list points at a path that no longer exists and
the script breaks for every developer (see
https://github.com/ray-project/ray/issues/26017).

The check resolves every entry the same way ``do_link`` does, relative to
``python/ray/setup-dev.py``, and verifies that the resolved path exists. It
runs from the lint pipeline, which has the full checkout. The Bazel tests for
``setup-dev.py`` only cover ``do_link`` behavior, because CI test images mask
most of the Ray libraries and Bazel runfiles do not contain files from
subpackages.

Usage:
    python ci/lint/check_setup_dev_packages.py
"""

import ast
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SETUP_DEV_PATH = os.path.join(REPO_ROOT, "python", "ray", "setup-dev.py")
PACKAGES_TO_LINK = "PACKAGES_TO_LINK"


def read_packages_to_link():
    """Return the ``PACKAGES_TO_LINK`` mapping defined in ``setup-dev.py``."""
    with open(SETUP_DEV_PATH, encoding="utf-8") as setup_dev_file:
        tree = ast.parse(setup_dev_file.read(), SETUP_DEV_PATH)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == PACKAGES_TO_LINK
            for target in node.targets
        ):
            try:
                return ast.literal_eval(node.value)
            except (ValueError, TypeError) as exc:
                raise RuntimeError(
                    f"Failed to parse {PACKAGES_TO_LINK} in {SETUP_DEV_PATH}. "
                    f"It must be a static literal dictionary: {exc}"
                )
    raise RuntimeError(f"{PACKAGES_TO_LINK} is not defined in {SETUP_DEV_PATH}")


def resolved_source_path(package, local_path):
    """Resolve a package entry the same way ``do_link`` does."""
    if local_path is None:
        local_path = f"../{package}"
    return os.path.abspath(os.path.join(SETUP_DEV_PATH, local_path))


def main():
    missing = []
    packages = read_packages_to_link()
    for package, local_path in packages.items():
        source_path = resolved_source_path(package, local_path)
        if not os.path.exists(source_path):
            missing.append(os.path.relpath(source_path, REPO_ROOT))
    if missing:
        print(
            "python/ray/setup-dev.py links paths that do not exist: "
            + ", ".join(sorted(missing)),
            file=sys.stderr,
        )
        return 1
    print(f"Checked {len(packages)} setup-dev.py packages; all exist.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
