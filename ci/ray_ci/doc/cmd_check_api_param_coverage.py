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
    # Decode as UTF-8 explicitly rather than relying on the locale default, so
    # non-ASCII paths in git output survive on any platform.
    return subprocess.check_output(
        ["git", "-C", checkout_dir, *args], encoding="utf-8", stderr=subprocess.DEVNULL
    )


def _repo_rel(path: str, checkout_dir: str) -> str:
    """Checkout-relative path with forward slashes, matching git's output."""
    return os.path.relpath(path, checkout_dir).replace(os.sep, "/")


def _merge_base(checkout_dir: str, base_ref: str) -> Optional[str]:
    try:
        return _git(checkout_dir, "merge-base", base_ref, "HEAD").strip() or None
    except subprocess.CalledProcessError:
        return None


def _in_scope(path: str) -> bool:
    """Whether a repo-relative path is a ``python/ray`` API source file."""
    return (
        path.endswith(".py")
        and path.startswith(f"{_SOURCE_ROOT}/")
        and not any(seg in f"/{path}" for seg in _SKIP_SEGMENTS)
    )


def _changed_python_files(checkout_dir: str, base: str) -> List[Tuple[str, str]]:
    """``(head_path, base_path)`` for ``python/ray`` sources changed since ``base``.

    Skips deleted files (no head content to check) and the non-API paths in
    ``_SKIP_SEGMENTS``. Rename detection is on (``-M``): for a renamed or copied
    file the two paths differ, so the base content is read from the old path
    rather than being treated as a new file. Without this a rename would report
    every pre-existing gap in the file as new debt.
    """
    try:
        out = _git(
            checkout_dir,
            "diff",
            "--name-status",
            "-M",
            "--diff-filter=d",
            f"{base}...HEAD",
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"could not list changed files: {e}")
    files = []
    for line in out.splitlines():
        fields = line.rstrip("\n").split("\t")
        if len(fields) < 2:
            continue
        status = fields[0]
        # Rename/copy entries carry both paths: "R100\told\tnew".
        if status[:1] in ("R", "C") and len(fields) >= 3:
            base_path, head_path = fields[1], fields[2]
        else:
            base_path = head_path = fields[1]
        if not _in_scope(head_path):
            continue
        files.append((head_path, base_path))
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
        rel_dirpath = _repo_rel(dirpath, checkout_dir)
        if any(seg in f"/{rel_dirpath}" for seg in _SKIP_SEGMENTS):
            continue
        for fn in filenames:
            if not fn.endswith(".py"):
                continue
            abspath = os.path.join(dirpath, fn)
            rel = _repo_rel(abspath, checkout_dir)
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
    # base index and the per-file comparison. Keyed by head path, but read from
    # the base path so a renamed file still compares against its old content.
    base_sources: Dict[str, Optional[str]] = {
        head_path: _base_content(checkout_dir, base, base_path)
        for head_path, base_path in changed
    }

    # Head index: the working tree. Base index: the working tree with the
    # changed files reverted to their base content (added files dropped). Only
    # the changed files differ between the two trees, so this reconstructs the
    # base tree accurately without a second checkout.
    head_files = list(_iter_source_files(checkout_dir))
    changed_set = {head_path for head_path, _ in changed}
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
    for path, _base_path in changed:
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
