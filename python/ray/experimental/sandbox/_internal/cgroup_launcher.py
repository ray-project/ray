"""Join an already configured sandbox cgroup before executing any workload.

Execute this file directly with Python -S. It must not import Ray, spawn
children, or start threads before entering the cgroup. Unlike preexec_fn,
this remains safe when the hosting worker has multiple threads.
"""

import os
import sys


def _cpus(value: str) -> set:
    cpus = set()
    for part in value.split(","):
        start, _, end = part.partition("-")
        cpus.update(range(int(start), int(end or start) + 1))
    return cpus


def main() -> None:
    cgroup, *command = sys.argv[1:]
    cpus = None
    if command[:1] == ["--cpus"]:
        cpus, command = _cpus(command[1]), command[2:]
    if not command:
        raise ValueError("A sandbox launch command is required")
    with os.fdopen(
        os.open(os.path.join(cgroup, "cgroup.procs"), os.O_WRONLY), "w"
    ) as stream:
        stream.write(str(os.getpid()))
    if cpus:
        # gVisor sizes the sandbox's vCPUs from these CPUs, and every process
        # the command starts inherits them.
        os.sched_setaffinity(0, cpus)
    os.execvp(command[0], command)


if __name__ == "__main__":
    main()
