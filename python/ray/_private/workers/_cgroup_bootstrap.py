"""Move this process into a cgroup, then run the given command.

This runs before Ray is imported, so the fork server's memory counts toward that cgroup.
"""

import os
import sys


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit(f"Usage: {sys.argv[0]} <cgroup_path> <executable> [args...]")
    fd = os.open(os.path.join(sys.argv[1], "cgroup.procs"), os.O_WRONLY)
    try:
        os.write(fd, str(os.getpid()).encode())
    finally:
        os.close(fd)
    os.execvp(sys.argv[2], sys.argv[2:])


if __name__ == "__main__":
    main()
