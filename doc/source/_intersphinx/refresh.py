#!/usr/bin/env python3
"""Refresh the committed intersphinx inventory snapshots.

Ray's Sphinx build resolves cross-references against roughly twenty third-party
``objects.inv`` inventories. Fetching them over the network at the start of
every build is slow and occasionally flaky (a couple go through GitHub
release-asset redirects to signed blob-storage URLs). To keep builds fast and
resilient we commit a snapshot of each inventory under this directory and point
``intersphinx_mapping`` in ``doc/source/conf.py`` at the local file first,
falling back to the network only if a snapshot is missing.

This script rebuilds those snapshots. A scheduled monthly job runs it and opens
a PR when anything drifted -- most targets resolve against upstream's moving
``stable`` / ``latest`` / ``main`` docs, so their inventories change when
upstream *releases*, not when Ray bumps a pin. (The three targets that set an
explicit inventory URL are exceptions; see this directory's README.) Run it by
hand after adding a target, or when a cross-reference to a symbol that does
exist upstream stops resolving::

    python doc/source/_intersphinx/refresh.py              # refresh all
    python doc/source/_intersphinx/refresh.py numpy torch  # refresh a subset

A refresh must land as a reviewed PR, never auto-merged: if upstream removed a
symbol Ray's docs reference, the stale snapshot was silently resolving it, and
the refresh PR's ``-W`` build is what surfaces the now-broken reference. See this
directory's README.

The list of projects and their upstream inventory locations is read directly
from ``_intersphinx_targets`` in ``doc/source/conf.py`` -- that mapping is the
single source of truth, so this script never drifts from the build config.

Review the resulting diff and re-run the docs build before committing.
"""

from __future__ import annotations

import argparse
import ast
import posixpath
from pathlib import Path

try:
    import requests
except ImportError as err:  # pragma: no cover
    raise SystemExit(
        "This script needs `requests` (already a docs-build dependency). "
        "Run it inside the docs virtualenv, e.g. after "
        "`pip install -r doc/requirements-doc.txt`."
    ) from err

# This file lives at doc/source/_intersphinx/refresh.py.
HERE = Path(__file__).resolve().parent
CONF_PY = HERE.parent / "conf.py"

# Matches sphinx.builders.html.INVENTORY_FILENAME.
INVENTORY_FILENAME = "objects.inv"
# objects.inv files begin with a plaintext version banner (v1 or v2).
INVENTORY_MAGIC = b"# Sphinx inventory version"
# Some hosts (raw.githubusercontent.com, release-asset redirects) are picky
# about a missing/empty User-Agent.
USER_AGENT = "ray-docs-intersphinx-refresh"
TIMEOUT = 60


def load_targets() -> "dict[str, tuple[str, str | None]]":
    """Return ``_intersphinx_targets`` (name -> (base_url, inventory)).

    We parse the literal out of conf.py rather than importing it: conf.py has
    heavy import-time side effects (it rewrites sys.path and registers custom
    Sphinx extensions).
    """
    # Explicit encoding: conf.py contains non-ASCII (em-dashes), and the default
    # locale encoding decodes it wrong on some Windows locales (silent mojibake
    # on cp1252, UnicodeDecodeError on cp932/gbk).
    tree = ast.parse(CONF_PY.read_text(encoding="utf-8"), filename=str(CONF_PY))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "_intersphinx_targets"
            for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise SystemExit(f"Could not find `_intersphinx_targets` assignment in {CONF_PY}")


def inventory_url(base_url: str, inventory: "str | None") -> str:
    """Resolve the upstream inventory URL for a target.

    Mirrors Sphinx's default: a ``None`` inventory means ``<base_url>objects.inv``
    joined exactly the way Sphinx joins it (``posixpath.join``).
    """
    if inventory:
        return inventory
    return posixpath.join(base_url, INVENTORY_FILENAME)


def fetch(url: str) -> bytes:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.content


def refresh(names: "list[str]") -> int:
    targets = load_targets()

    unknown = [n for n in names if n not in targets]
    if unknown:
        raise SystemExit(
            f"Unknown intersphinx target(s): {', '.join(unknown)}\n"
            f"Known targets: {', '.join(sorted(targets))}"
        )
    selected = names or sorted(targets)

    failures: "list[str]" = []
    for name in selected:
        base_url, inventory = targets[name]
        url = inventory_url(base_url, inventory)
        dest = HERE / f"{name}.inv"
        try:
            data = fetch(url)
        except (requests.RequestException, OSError) as err:
            print(f"  FAIL {name}\n       {url}\n       {err}")
            failures.append(name)
            continue
        if not data.startswith(INVENTORY_MAGIC):
            print(
                f"  FAIL {name}\n       {url}\n"
                f"       not a Sphinx inventory (starts with {data[:40]!r})"
            )
            failures.append(name)
            continue
        # Atomic write so an interrupted run never leaves a partial snapshot.
        # A local I/O error (disk full, read-only checkout) is reported the same
        # way as a download failure so one bad target can't abort the rest, and
        # the temp file is removed so a failed run leaves no stray .tmp behind.
        tmp = dest.with_name(dest.name + ".tmp")
        try:
            tmp.write_bytes(data)
            tmp.replace(dest)
        except OSError as err:
            tmp.unlink(missing_ok=True)
            print(f"  FAIL {name}\n       {dest}\n       {err}")
            failures.append(name)
            continue
        print(f"  ok   {name:<20} {len(data):>9,d} bytes  <- {url}")

    if failures:
        print(f"\n{len(failures)} inventory(ies) failed: {', '.join(failures)}")
        return 1
    print(f"\nRefreshed {len(selected)} inventory(ies) into {HERE}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh committed intersphinx inventory snapshots."
    )
    parser.add_argument(
        "names",
        nargs="*",
        metavar="TARGET",
        help="Specific intersphinx target(s) to refresh (default: all).",
    )
    args = parser.parse_args()
    return refresh(args.names)


if __name__ == "__main__":
    raise SystemExit(main())
