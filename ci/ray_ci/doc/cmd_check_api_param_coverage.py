import argparse
import os
import subprocess
import sys
from typing import Dict, List, Optional, Tuple

from ci.ray_ci.doc.api_param_coverage import (
    Violation,
    build_class_index,
    new_violations_for_file,
)

# Only the Python API surface is in scope. Mirror the audit's skip list so
# tests, examples, and vendored code never enter the index or the diff.
_SOURCE_ROOT = "python/ray"
_SKIP_SEGMENTS = (
    "/tests",
    "/test",
    "/examples",
    "/_private/thirdparty",
    "/dashboard/client",
)


def _git(checkout_dir: str, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", checkout_dir, *args], text=True, stderr=subprocess.DEVNULL
    )


def _merge_base(checkout_dir: str, base_ref: str) -> Optional[str]:
    try:
        return _git(checkout_dir, "merge-base", base_ref, "HEAD").strip() or None
    except subprocess.CalledProcessError:
        return None


def _changed_python_files(checkout_dir: str, base: str) -> List[str]:
    """Repo-root-relative ``python/ray`` source files changed since ``base``.

    Skips deleted files (no head content to check) and the non-API paths in
    ``_SKIP_SEGMENTS``.
    """
    try:
        out = _git(
            checkout_dir, "diff", "--name-only", "--diff-filter=d", f"{base}...HEAD"
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"could not list changed files: {e}")
    files = []
    for line in out.splitlines():
        path = line.strip()
        if not path.endswith(".py"):
            continue
        if not path.startswith(f"{_SOURCE_ROOT}/"):
            continue
        if any(seg in f"/{path}" for seg in _SKIP_SEGMENTS):
            continue
        files.append(path)
    return files


def _base_content(checkout_dir: str, base: str, path: str) -> Optional[str]:
    """File content at the base revision, or None if it did not exist there."""
    try:
        return _git(checkout_dir, "show", f"{base}:{path}")
    except subprocess.CalledProcessError:
        return None


def _iter_source_files(checkout_dir: str):
    """Yield ``(repo_rel_path, source)`` for every in-scope working-tree file."""
    root = os.path.join(checkout_dir, _SOURCE_ROOT)
    for dirpath, _dirs, filenames in os.walk(root):
        # Match skip segments against the repo-relative path, not the absolute
        # one: a checkout dir that itself contains a skip segment (e.g. a path
        # under ".../test/...") would otherwise skip every file.
        rel_dirpath = os.path.relpath(dirpath, checkout_dir)
        if any(seg in f"/{rel_dirpath}" for seg in _SKIP_SEGMENTS):
            continue
        for fn in filenames:
            if not fn.endswith(".py"):
                continue
            abspath = os.path.join(dirpath, fn)
            rel = os.path.relpath(abspath, checkout_dir)
            try:
                with open(abspath, encoding="utf-8") as f:
                    yield rel, f.read()
            except (OSError, UnicodeDecodeError):
                continue


def find_violations(checkout_dir: str, base_ref: str) -> Tuple[List[Violation], str]:
    """Run the diff-scoped coverage check. Returns ``(violations, base_sha)``.

    Raises RuntimeError when the base revision cannot be resolved (fail-closed
    responsibility is left to the caller so it can honor the warn/blocking
    posture).
    """
    base = _merge_base(checkout_dir, base_ref)
    if base is None:
        raise RuntimeError(
            f"could not determine merge-base between {base_ref} and HEAD"
        )

    changed = _changed_python_files(checkout_dir, base)
    if not changed:
        return [], base

    # Base content of the changed files, fetched once and reused for both the
    # base index and the per-file comparison.
    base_sources: Dict[str, Optional[str]] = {
        path: _base_content(checkout_dir, base, path) for path in changed
    }

    # Head index: the working tree. Base index: the working tree with the
    # changed files reverted to their base content (added files dropped). Only
    # the changed files differ between the two trees, so this reconstructs the
    # base tree accurately without a second checkout.
    head_files = list(_iter_source_files(checkout_dir))
    changed_set = set(changed)
    base_files = []
    for rel, source in head_files:
        if rel in changed_set:
            base_src = base_sources[rel]
            if base_src is not None:
                base_files.append((rel, base_src))
        else:
            base_files.append((rel, source))

    head_index = build_class_index(head_files)
    base_index = build_class_index(base_files)

    head_by_path = dict(head_files)
    violations: List[Violation] = []
    for path in changed:
        head_source = head_by_path.get(path)
        if head_source is None:
            continue
        violations.extend(
            new_violations_for_file(
                path,
                base_sources[path],
                head_source,
                base_index,
                head_index,
            )
        )
    violations.sort(key=lambda v: (v.path, v.lineno, v.qualname))
    return violations, base


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse the command line.

    Uses the standard library only: this check runs in the plain lint container,
    which has no third-party dependencies installed.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Fail a pull request that adds a new @PublicAPI callable, or a new "
            "parameter on an existing one, without a docstring Args: entry. "
            "Pre-existing gaps are grandfathered; only newly-undocumented params "
            "on the changed public surface are reported. Static: parses source, "
            "no Ray build or import needed."
        )
    )
    parser.add_argument(
        "ray_checkout_dir",
        help="Path to the Ray checkout to scan.",
    )
    parser.add_argument(
        "--base-ref",
        default="origin/master",
        help="Git ref for the pull-request base branch. (default: origin/master)",
    )
    parser.add_argument(
        "--blocking",
        action="store_true",
        help=(
            "Exit non-zero on violations. Off by default (warn only) so the "
            "false-positive rate can be confirmed before the check becomes "
            "required."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)
    ray_checkout_dir = args.ray_checkout_dir
    base_ref = args.base_ref
    blocking = args.blocking

    try:
        violations, base = find_violations(ray_checkout_dir, base_ref)
    except RuntimeError as e:
        # Fail-closed only when blocking; in warn mode a missing base branch
        # must not break the build.
        print(f"--- API param coverage: {e}", file=sys.stderr)
        sys.exit(1 if blocking else 0)

    print(
        f"--- Checking new-parameter documentation coverage against {base[:12]}...",
        file=sys.stderr,
    )

    if not violations:
        print("No newly-undocumented public-API parameters. ", file=sys.stderr)
        return

    print(
        "Public APIs with newly-undocumented parameters "
        "(add an Args: entry for each):",
        file=sys.stderr,
    )
    for v in violations:
        params = ", ".join(v.params)
        print(f"\t{v.path}:{v.lineno}  {v.qualname}  ->  {params}", file=sys.stderr)

    total = sum(len(v.params) for v in violations)
    print(
        f"\n{total} newly-undocumented parameter(s) across {len(violations)} "
        "public callable(s). Document each parameter in the callable's docstring "
        "Args: block (for __init__, the class docstring). Pre-existing gaps are "
        "grandfathered; this gate fires only on new or changed public API.",
        file=sys.stderr,
    )

    if blocking:
        sys.exit(1)
    print(
        "\n(non-blocking: reporting only. This check does not fail the build yet.)",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
