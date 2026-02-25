import logging

from ray._private.utils import get_num_cpus

logger = logging.getLogger(__name__)

CPU_USAGE_PATH = "/sys/fs/cgroup/cpuacct/cpuacct.usage"
CPU_USAGE_PATH_V2 = "/sys/fs/cgroup/cpu.stat"
PROC_STAT_PATH = "/proc/stat"

container_num_cpus = None
host_num_cpus = None

last_cpu_usage = None
last_system_usage = None


def cpu_percent():
    """Estimate CPU usage percent for Ray pod managed by Kubernetes
    Operator.

    Computed by the following steps
       (1) Replicate the logic used by 'docker stats' cli command.
           See https://github.com/docker/cli/blob/c0a6b1c7b30203fbc28cd619acb901a95a80e30e/cli/command/container/stats_helpers.go#L166.
       (2) Divide by the number of CPUs available to the container, so that
           e.g. full capacity use of 2 CPUs will read as 100%,
           rather than 200%.

    Step (1) above works by
        dividing delta in cpu usage by
        delta in total host cpu usage, averaged over host's cpus.

    Since deltas are not initially available, return 0.0 on first call.
    """  # noqa
    global last_system_usage
    global last_cpu_usage
    try:
        cpu_usage = _cpu_usage()
        system_usage = _system_usage()
        # Return 0.0 on first call.
        if last_system_usage is None:
            cpu_percent = 0.0
        else:
            cpu_delta = cpu_usage - last_cpu_usage
            # "System time passed." (Typically close to clock time.)
            system_delta = (system_usage - last_system_usage) / _host_num_cpus()

            quotient = cpu_delta / system_delta
            cpu_percent = round(quotient * 100 / get_num_cpus(), 1)
        last_system_usage = system_usage
        last_cpu_usage = cpu_usage
        # Computed percentage might be slightly above 100%.
        return min(cpu_percent, 100.0)
    except Exception:
        logger.exception("Error computing CPU usage of Ray Kubernetes pod.")
        return 0.0


def _cpu_usage():
    """Compute total cpu usage of the container in nanoseconds
    by reading from cpuacct in cgroups v1 or cpu.stat in cgroups v2."""
    try:
        # cgroups v1
        return int(open(CPU_USAGE_PATH).read())
    except FileNotFoundError:
        # cgroups v2
        cpu_stat_text = open(CPU_USAGE_PATH_V2).read()
        # e.g. "usage_usec 16089294616"
        cpu_stat_first_line = cpu_stat_text.split("\n")[0]
        # get the second word of the first line, cast as an integer
        # this is the CPU usage is microseconds
        cpu_usec = int(cpu_stat_first_line.split()[1])
        # Convert to nanoseconds and return.
        return cpu_usec * 1000


def _system_usage():
    """
    Computes total CPU usage of the host in nanoseconds.

    Logic taken from here:
    https://github.com/moby/moby/blob/b42ac8d370a8ef8ec720dff0ca9dfb3530ac0a6a/daemon/stats/collector_unix.go#L31

    See also the /proc/stat entry here:
    https://man7.org/linux/man-pages/man5/proc.5.html
    """  # noqa
    cpu_summary_str = open(PROC_STAT_PATH).read().split("\n")[0]
    parts = cpu_summary_str.split()
    assert parts[0] == "cpu"
    usage_data = parts[1:8]
    total_clock_ticks = sum(int(entry) for entry in usage_data)
    # 100 clock ticks per second, 10^9 ns per second
    usage_ns = total_clock_ticks * 10**7
    return usage_ns


def _host_num_cpus():
    """Number of physical CPUs, obtained by parsing /proc/stat."""
    global host_num_cpus
    if host_num_cpus is None:
        proc_stat_lines = open(PROC_STAT_PATH).read().split("\n")
        split_proc_stat_lines = [line.split() for line in proc_stat_lines]
        cpu_lines = [
            split_line
            for split_line in split_proc_stat_lines
            if len(split_line) > 0 and "cpu" in split_line[0]
        ]
        # Number of lines starting with a word including 'cpu', subtracting
        # 1 for the first summary line.
        host_num_cpus = len(cpu_lines) - 1
    return host_num_cpus
