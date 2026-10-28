"""Stdlib-only pre-exec bootstrapper to place fork servers in the workers cgroup.

Invoked with ``python -S _cgroup_bootstrap.py <cgroup_path> <cmd...>`` so site.py and
Ray packages are not imported inside the caller's ``system`` cgroup. Writing to
``cgroup.procs`` followed immediately by ``os.execvp`` causes Linux ``exec_mmap()`` to
tear down the pre-exec address space, charging all subsequent interpreter and preloaded
module pages directly to ``user/workers``.
"""

import os
import sys


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit(f"Usage: {sys.argv[0]} <cgroup_path> <executable> [args...]")
    cgroup_procs = os.path.join(sys.argv[1], "cgroup.procs")
    with os.fdopen(
        os.open(cgroup_procs, os.O_WRONLY | os.O_CLOEXEC), "wb", buffering=0
    ) as stream:
        stream.write(str(os.getpid()).encode("ascii"))
    os.execvp(sys.argv[2], sys.argv[2:])


if __name__ == "__main__":
    main()
