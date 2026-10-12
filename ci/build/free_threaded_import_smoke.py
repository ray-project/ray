#!/usr/bin/env python3
"""Imports Ray in a free-threaded interpreter and reports whether the GIL came back.

Informational: a re-enabled GIL is reported but does not fail the check; only a
failed import does. Runs without PYTHON_GIL so the interpreter's own behavior shows.
"""
import os
import re
import subprocess
import sys
import sysconfig
from dataclasses import dataclass
from typing import Optional

_GIL_WARNING = re.compile(
    r"The global interpreter lock \(GIL\) has been enabled to load module '([^']+)'"
)


@dataclass
class ImportResult:
    returncode: int
    gil_enabled: Optional[bool]
    gil_enabling_module: Optional[str]
    stderr: str


def find_gil_enabling_module(stderr: str) -> Optional[str]:
    match = _GIL_WARNING.search(stderr)
    return match.group(1) if match else None


def check_import(python: str, module: str) -> ImportResult:
    # sys._is_gil_enabled only exists on 3.13+; older interpreters always have a GIL.
    code = (
        f"import sys, {module}; "
        "print(getattr(sys, '_is_gil_enabled', lambda: True)())"
    )
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHON_GIL", "PYTHONPATH")}
    proc = subprocess.run([python, "-c", code], env=env, capture_output=True, text=True)
    gil_enabled = None
    if proc.returncode == 0:
        gil_enabled = proc.stdout.strip().endswith("True")
    return ImportResult(
        proc.returncode,
        gil_enabled,
        find_gil_enabling_module(proc.stderr),
        proc.stderr,
    )


def main() -> int:
    if not sysconfig.get_config_var("Py_GIL_DISABLED"):
        print(f"{sys.executable} is not a free-threaded build", file=sys.stderr)
        return 1
    result = check_import(sys.executable, "ray")
    if result.returncode != 0:
        print(f"FAILED: import ray\n{result.stderr}", file=sys.stderr)
        return 1
    if result.gil_enabled:
        print(
            "GIL re-enabled on import ray"
            f" by module: {result.gil_enabling_module or 'unknown'}"
        )
    else:
        print("GIL stays disabled on import ray")
    return 0


if __name__ == "__main__":
    sys.exit(main())
