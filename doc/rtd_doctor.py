#!/usr/bin/env python
"""Preflight check that the local environment matches the Read the Docs build.

Ray's docs build on Read the Docs with ``fail_on_warning: true``, so a local
build is only trustworthy when it reproduces RtD's environment. This doctor
compares the active interpreter and installed packages against what RtD uses
and reports any drift before ``make rtd`` spends minutes on a build that
wouldn't be faithful anyway.

The expected values are read from the repo, never hardcoded, so this keeps
working as versions are tweaked:

  * Python version  <- ``.readthedocs.yaml`` ``build.tools.python``
  * pinned deps     <- ``doc/requirements-doc.lock.txt`` (the docs lock)

This script is intentionally stdlib-only: it diagnoses a possibly-broken or
mismatched environment, so it must not depend on the very packages it checks
(it uses PyYAML only if it happens to be importable, with a regex fallback).

Exit status: 0 when faithful (or under ``--warn-only``), 1 on critical drift,
2 when the expected values can't be determined (a repo/config problem).
"""

# Annotations are lazy strings so this runs under any interpreter a contributor
# might have active, even one older than the docs build's Python.
from __future__ import annotations

import argparse
import platform
import re
import sys
from importlib import metadata
from pathlib import Path

# Optional: use PyYAML for a structural parse when present, but never depend on
# it — this script must run in an environment that's missing the docs deps.
try:
    import yaml
except ImportError:
    yaml = None

# doc/rtd_doctor.py -> doc/ -> repo root
DOC_DIR = Path(__file__).resolve().parent
RAY_ROOT = DOC_DIR.parent
RTD_YAML = RAY_ROOT / ".readthedocs.yaml"
DOC_LOCK = DOC_DIR / "requirements-doc.lock.txt"

# RtD runs on this OS (informational only; see .readthedocs.yaml build.os).
RTD_OS = "ubuntu-24.04"

# Packages whose pinned version most directly determines build fidelity. The
# names are stable; their expected versions come from the lock, not from here.
KEY_PACKAGES = ["sphinx", "myst-parser", "myst-nb", "pydata-sphinx-theme"]

# Status tiers. CRITICAL fails the check (and blocks `make rtd`); WARN and INFO
# never do.
CRITICAL, WARN, INFO, OK = "critical", "warn", "info", "ok"


class DoctorError(Exception):
    """The doctor couldn't determine what to check (a repo/config problem)."""


def _normalize(name: str) -> str:
    """Normalize a distribution name per PEP 503 for comparison."""
    return re.sub(r"[-_.]+", "-", name).lower()


def read_expected_python() -> str:
    """Return the Python version RtD builds with, from .readthedocs.yaml."""
    if not RTD_YAML.is_file():
        raise DoctorError(f"{RTD_YAML} not found")
    text = RTD_YAML.read_text()

    # Prefer a structural parse when PyYAML is available; fall back to a
    # targeted scan so the doctor still runs in a stripped-down environment.
    if yaml is not None:
        try:
            data = yaml.safe_load(text)
            value = data["build"]["tools"]["python"]
            return str(value).strip()
        except (KeyError, TypeError, AttributeError):
            pass  # fall through to the regex scan

    # The first `python:` after `tools:` is build.tools.python; the top-level
    # `python:` install block appears later in the file.
    match = re.search(r"tools:.*?\bpython:\s*[\"']?(\d+(?:\.\d+)?)", text, re.DOTALL)
    if not match:
        raise DoctorError(f"could not find build.tools.python in {RTD_YAML.name}")
    return match.group(1)


def read_lock_versions() -> dict[str, str]:
    """Return {normalized name: version} parsed from the docs lock."""
    if not DOC_LOCK.is_file():
        raise DoctorError(f"{DOC_LOCK} not found")
    versions = {}
    # Lock lines look like `sphinx==8.2.3 \`; --hash/# via/--index-url lines
    # don't start with a package-name character, so anchoring excludes them.
    pin = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s\\]+)")
    for line in DOC_LOCK.read_text().splitlines():
        match = pin.match(line)
        if match:
            versions[_normalize(match.group(1))] = match.group(2)
    if not versions:
        raise DoctorError(f"no pinned versions parsed from {DOC_LOCK.name}")
    return versions


def installed_version(name: str) -> str | None:
    """Return the installed version of ``name``, or None if absent."""
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def lock_target_python() -> str | None:
    """Best-effort Python tag from the resolved lock filename (e.g. '3.11')."""
    try:
        name = DOC_LOCK.resolve().name
    except OSError:
        return None
    match = re.search(r"py(\d+)\.?(\d+)", name)
    return f"{match.group(1)}.{match.group(2)}" if match else None


def build_findings() -> tuple[list[tuple[str, str, str, str]], str]:
    """Run every check, returning (rows, expected_python).

    Each row is (axis, expected, actual, status). Raises DoctorError if the
    expected values themselves can't be read.
    """
    expected_python = read_expected_python()
    lock = read_lock_versions()

    rows = []

    actual_python = f"{sys.version_info.major}.{sys.version_info.minor}"
    rows.append(
        (
            "Python",
            expected_python,
            actual_python,
            OK if actual_python == expected_python else CRITICAL,
        )
    )

    for pkg in KEY_PACKAGES:
        expected = lock.get(_normalize(pkg))
        if expected is None:
            rows.append((pkg, "(not in lock)", "-", WARN))
            continue
        actual = installed_version(pkg)
        if actual is None:
            rows.append((pkg, expected, "(not installed)", CRITICAL))
        else:
            rows.append((pkg, expected, actual, OK if actual == expected else CRITICAL))

    # Informational / repo-consistency axes — never block.
    rows.append(("operating system", RTD_OS, platform.platform(), INFO))
    lock_py = lock_target_python()
    if lock_py is not None and lock_py != expected_python:
        rows.append(("docs lock target", f"py{expected_python}", f"py{lock_py}", WARN))

    return rows, expected_python


def render(rows: list[tuple[str, str, str, str]], warn_only: bool, quiet: bool) -> int:
    """Print the report and return the process exit code."""
    criticals = [r for r in rows if r[3] == CRITICAL]
    warnings = [r for r in rows if r[3] == WARN]

    status_label = {
        OK: "ok",
        CRITICAL: "MISMATCH" if warn_only else "MISMATCH (critical)",
        WARN: "check",
        INFO: "differs (low risk)",
    }

    if not quiet:
        print("Read the Docs build fidelity check")
        print(f"  expected Python from {RTD_YAML.name} (build.tools.python)")
        print(f"  expected deps   from {DOC_LOCK.name} (the docs lock)")
        print()
        axis_w = max(len(r[0]) for r in rows)
        exp_w = max(len(str(r[1])) for r in rows)
        act_w = max(len(str(r[2])) for r in rows)
        for axis, expected, actual, status in rows:
            print(
                f"  {axis:<{axis_w}}  {str(expected):<{exp_w}}  "
                f"{str(actual):<{act_w}}  {status_label[status]}"
            )
        print()

    if criticals and not warn_only:
        print(
            f"✗ Environment does not match the Read the Docs build "
            f"({len(criticals)} critical issue(s))."
        )
        _print_fixes(criticals)
        return 1

    if criticals:  # warn_only: surface them but don't block
        print(
            f"! {len(criticals)} environment mismatch(es) (continuing anyway "
            f"because --warn-only). The local build may not match RtD."
        )
        _print_fixes(criticals)
        return 0

    if warnings:
        print(
            "✓ Environment matches the Read the Docs build "
            f"({len(warnings)} non-blocking warning(s) above)."
        )
        return 0

    print("✓ Environment matches the Read the Docs build.")
    return 0


def _print_fixes(criticals: list[tuple[str, str, str, str]]) -> None:
    """Print actionable remediation for the critical findings."""
    axes = {r[0] for r in criticals}
    print("  Fix:")
    if "Python" in axes:
        expected = next(r[1] for r in criticals if r[0] == "Python")
        print(
            f"    - Use Python {expected} (RtD's version). Recreate your env, "
            f"e.g. `conda create -n docs python={expected}`."
        )
    if axes - {"Python"}:  # any dependency drift
        print(
            "    - Reinstall the pinned docs deps without -U, from the doc/ "
            "directory:"
        )
        print("        pip install -r requirements-doc.lock.txt")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Check that this environment matches the Read the Docs build "
            "(Python version and pinned docs dependencies)."
        )
    )
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Report drift as warnings and exit 0 instead of failing.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Print only the verdict and any fixes, not the full table.",
    )
    args = parser.parse_args(argv)

    try:
        rows, _ = build_findings()
    except DoctorError as exc:
        if args.warn_only:
            print(
                f"! Could not run the RtD fidelity check: {exc} "
                f"(continuing because --warn-only)."
            )
            return 0
        print(f"✗ Could not run the RtD fidelity check: {exc}")
        return 2

    return render(rows, warn_only=args.warn_only, quiet=args.quiet)


if __name__ == "__main__":
    sys.exit(main())
