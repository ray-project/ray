import re
import sys
from unittest import mock
import tempfile
import time

import pytest
import requests

import ray
from ray.new_dashboard import k8s_utils

# Some experimentally-obtained K8S CPU usage files for use in test_k8s_cpu.
PROCSTAT1 = \
"""cpu  2945022 98 3329420 148744854 39522 0 118587 0 0 0
cpu0 370299 14 413841 18589778 5304 0 15288 0 0 0
cpu1 378637 10 414414 18589275 5283 0 14731 0 0 0
cpu2 367328 8 420914 18590974 4844 0 14416 0 0 0
cpu3 368378 11 423720 18572899 4948 0 14394 0 0 0
cpu4 369051 13 414615 18607285 4736 0 14383 0 0 0
cpu5 362958 10 415984 18576655 4590 0 16614 0 0 0
cpu6 362536 13 414430 18605197 4785 0 14353 0 0 0
cpu7 365833 15 411499 18612787 5028 0 14405 0 0 0
intr 1000694027 125 0 0 39 154 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1028 0 2160913 0 2779605 8 0 3981333 3665198 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
ctxt 1574979439
btime 1615208601
processes 857411
procs_running 6
procs_blocked 0
softirq 524311775 0 230142964 27143 63542182 0 0 171 74042767 0 156556548
""" # noqa

PROCSTAT2 = \
"""cpu  2945152 98 3329436 148745483 39522 0 118587 0 0 0
cpu0 370399 14 413841 18589778 5304 0 15288 0 0 0
cpu1 378647 10 414415 18589362 5283 0 14731 0 0 0
cpu2 367329 8 420916 18591067 4844 0 14416 0 0 0
cpu3 368381 11 423724 18572989 4948 0 14395 0 0 0
cpu4 369052 13 414618 18607374 4736 0 14383 0 0 0
cpu5 362968 10 415986 18576741 4590 0 16614 0 0 0
cpu6 362537 13 414432 18605290 4785 0 14353 0 0 0
cpu7 365836 15 411502 18612878 5028 0 14405 0 0 0
intr 1000700905 125 0 0 39 154 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1028 0 2160923 0 2779605 8 0 3981353 3665218 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
ctxt 1574988760
btime 1615208601
processes 857411
procs_running 4
procs_blocked 0
softirq 524317451 0 230145523 27143 63542930 0 0 171 74043232 0 156558452
""" # noqa

CPUACCTUSAGE1 = "2268980984108"

CPUACCTUSAGE2 = "2270120061999"

CPU_QUOTA = "-1"

CPU_PERIOD = "1000"

CPUSETS = "0-7"

CPUSHARES = "2048"


@pytest.mark.skipif(
    sys.version_info < (3, 5, 3), reason="requires python3.5.3 or higher")
def test_dashboard(shutdown_only):
    addresses = ray.init(include_dashboard=True, num_cpus=1)
    dashboard_url = addresses["webui_url"]
    assert ray.get_dashboard_url() == dashboard_url

    assert re.match(r"^(localhost|\d+\.\d+\.\d+\.\d+):\d+$", dashboard_url)

    start_time = time.time()
    while True:
        try:
            node_info_url = f"http://{dashboard_url}/nodes"
            resp = requests.get(node_info_url, params={"view": "summary"})
            resp.raise_for_status()
            summaries = resp.json()
            assert summaries["result"] is True
            assert "msg" in summaries
            break
        except (requests.exceptions.ConnectionError, AssertionError):
            if time.time() > start_time + 30:
                out_log = None
                with open(
                        "{}/logs/dashboard.log".format(
                            addresses["session_dir"]), "r") as f:
                    out_log = f.read()
                raise Exception(
                    "Timed out while waiting for dashboard to start. "
                    f"Dashboard output log: {out_log}\n")


@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="No need to test on Windows.")
def test_k8s_cpu():
    """Test all the functions in k8s_utils.py.
    Also test ray.utils.get_num_cpus in the context of a docker container with
    cpu quota unset.

    Files were obtained from within a K8s pod with 2 CPU request, limit unset
    with 1 CPU of stress applied.
    """
    (shares_file, cpuset_file, cpu_period_file, cpu_quota_file, cpuacct_file,
     proc_stat_file) = [tempfile.NamedTemporaryFile("w+") for _ in range(6)]
    shares_file.write(CPUSHARES)
    cpuset_file.write(CPUSETS)
    cpu_period_file.write(CPU_PERIOD)
    cpu_quota_file.write(CPU_QUOTA)

    cpuacct_file.write(CPUACCTUSAGE1)
    proc_stat_file.write(PROCSTAT1)

    for file in shares_file, cpuacct_file, proc_stat_file:
        file.flush()

    patch_defaults = (cpu_quota_file.name, cpu_period_file.name,
                      cpuset_file.name, shares_file.name)

    with mock.patch("ray.new_dashboard.k8s_utils.CPU_USAGE_PATH",
                    cpuacct_file.name),\
            mock.patch("ray.new_dashboard.k8s_utils.CPU_USAGE_PATH",
                       cpuacct_file.name),\
            mock.patch("ray.new_dashboard.k8s_utils.PROC_STAT_PATH",
                       proc_stat_file.name),\
            mock.patch("ray.new_dashboard.k8s_utils.ray.utils."
                       "_get_docker_cpus.__defaults__", patch_defaults):

        # Test helpers
        assert k8s_utils.ray.utils.get_num_cpus() == 2
        assert k8s_utils._cpu_usage() == 2268980984108
        assert k8s_utils._system_usage() == 1551775030000000
        assert k8s_utils._host_num_cpus() == 8

        # No delta for first computation, return 0.
        assert k8s_utils.cpu_percent() == 0.0

        # Write new usage info obtained after 1 sec wait.
        for file in cpuacct_file, proc_stat_file:
            # Delete file and reset pointer
            file.truncate(0)
            file.seek(0)
        cpuacct_file.write(CPUACCTUSAGE2)
        proc_stat_file.write(PROCSTAT2)
        for file in cpuacct_file, proc_stat_file:
            file.flush()

        # Files were extracted under 1 CPU of load on a 2 CPU pod
        assert 50 < k8s_utils.cpu_percent() < 60


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
