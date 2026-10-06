"""Derive the py3.14 parity constraints from requirements_compiled.txt.

requirements_compiled.txt is resolved under Python 3.11. Compiling the py3.14
constraints against it keeps every package on the same version as 3.10-3.13,
except where 3.14 has to differ. Those differences are expressed in the source
requirements files with `python_version` markers, which pip-compile carries
into requirements_compiled.txt. This script drops:

  1. pins whose marker is false on Python 3.14 (the source split them), and
  2. pins required only by packages dropped in (1) or (2) and by no source
     file directly, so a companion follows its parent onto the 3.14 branch
     (e.g. pydantic-core follows pydantic).

Usage: python ci/py314_parity_constraints.py <requirements_compiled.txt> <output>
"""

import re
import sys

from packaging.markers import Marker

PY314_LINUX_ENV = {
    "python_version": "3.14",
    "python_full_version": "3.14.0",
    "implementation_name": "cpython",
    "platform_python_implementation": "CPython",
    "os_name": "posix",
    "sys_platform": "linux",
    "platform_system": "Linux",
    "platform_machine": "x86_64",
}

_PIN = re.compile(r"^([A-Za-z0-9_.-]+)==(\S+)(?:\s*;\s*(.*))?$")


def _normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def parse(path: str) -> dict:
    """Map each pinned package to its line, marker, parents and source flag."""
    entries = {}
    current = None
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            match = _PIN.match(line)
            if match:
                current = _normalize(match.group(1))
                entries[current] = {
                    "line": line,
                    "marker": match.group(3),
                    "parents": set(),
                    "from_source": False,
                }
            elif current and line.startswith("    #"):
                via = line.lstrip(" #").removeprefix("via").strip()
                if via.startswith(("-r ", "-c ")):
                    entries[current]["from_source"] = True
                elif via:
                    entries[current]["parents"].add(_normalize(via))
            elif not line.startswith(" "):
                current = None
    return entries


def dropped_on_py314(entries: dict) -> set:
    dropped = {
        name
        for name, entry in entries.items()
        if entry["marker"] and not Marker(entry["marker"]).evaluate(PY314_LINUX_ENV)
    }
    changed = True
    while changed:
        changed = False
        for name, entry in entries.items():
            if name in dropped or entry["from_source"] or not entry["parents"]:
                continue
            if entry["parents"] <= dropped:
                dropped.add(name)
                changed = True
    return dropped


def main(src: str, out: str) -> None:
    entries = parse(src)
    dropped = dropped_on_py314(entries)
    with open(out, "w") as f:
        for name, entry in entries.items():
            if name not in dropped:
                f.write(entry["line"] + "\n")
    print(
        f"py3.14 parity constraints: kept {len(entries) - len(dropped)}, "
        f"released {len(dropped)}: {' '.join(sorted(dropped))}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main(*sys.argv[1:3])
