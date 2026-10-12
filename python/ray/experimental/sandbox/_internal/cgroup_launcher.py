"""Join an already configured sandbox cgroup before executing any workload.

Execute this file directly with Python -S. It must not import Ray, spawn
children, or start threads before entering the cgroup. Unlike preexec_fn,
this remains safe when the hosting worker has multiple threads.
"""

import os
import sys


def main() -> None:
    cgroup, *command = sys.argv[1:]
    if not command:
        raise ValueError("A sandbox launch command is required")
    with os.fdopen(
        os.open(os.path.join(cgroup, "cgroup.procs"), os.O_WRONLY), "w"
    ) as stream:
        stream.write(str(os.getpid()))
    os.execvp(command[0], command)


if __name__ == "__main__":
    main()
