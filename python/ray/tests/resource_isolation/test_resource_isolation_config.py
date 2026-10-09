import contextlib
import os
import subprocess
import sys
import time
from types import SimpleNamespace
from typing import Dict, List

import pytest

import ray._private.ray_constants as ray_constants
from ray._common import utils as common_utils
from ray._private import utils
from ray._private.node import Node
from ray._private.resource_isolation_config import ResourceIsolationConfig

import psutil


def _fake_process_entry(pid: int) -> List[SimpleNamespace]:
    """Mimics the shape Node.all_processes holds: [ProcessInfo] with .process.pid."""
    return [SimpleNamespace(process=SimpleNamespace(pid=pid))]


def _all_processes(
    gcs_pid: int, dashboard_pid: int
) -> Dict[str, List[SimpleNamespace]]:
    return {
        ray_constants.PROCESS_TYPE_GCS_SERVER: _fake_process_entry(gcs_pid),
        ray_constants.PROCESS_TYPE_DASHBOARD: _fake_process_entry(dashboard_pid),
    }


def test_resource_isolation_is_disabled_by_default():
    resource_isolation_config = ResourceIsolationConfig()
    assert not resource_isolation_config.is_enabled()


def test_disabled_resource_isolation_with_overrides_raises_value_error():

    with pytest.raises(
        ValueError,
        match="cgroup_path cannot be set when resource isolation is not enabled",
    ):
        ResourceIsolationConfig(
            enable_resource_isolation=False, cgroup_path="/some/path"
        )

    with pytest.raises(
        ValueError,
        match="system_reserved_cpu cannot be set when resource isolation is not enabled",
    ):
        ResourceIsolationConfig(enable_resource_isolation=False, system_reserved_cpu=1)

    with pytest.raises(
        ValueError,
        match="system_reserved_cpu cannot be set when resource isolation is not enabled",
    ):
        ResourceIsolationConfig(enable_resource_isolation=False, system_reserved_cpu=0)

    with pytest.raises(
        ValueError,
        match="system_reserved_memory cannot be set when resource isolation is not enabled",
    ):
        ResourceIsolationConfig(
            enable_resource_isolation=False, system_reserved_memory=1024**3
        )

    with pytest.raises(
        ValueError,
        match="system_reserved_memory cannot be set when resource isolation is not enabled",
    ):
        ResourceIsolationConfig(
            enable_resource_isolation=False, system_reserved_memory=0
        )


def test_enabled_resource_isolation_with_non_string_cgroup_path_raises_value_error():

    with pytest.raises(ValueError, match="Invalid value.*for cgroup_path"):
        ResourceIsolationConfig(enable_resource_isolation=True, cgroup_path=1)

    with pytest.raises(ValueError, match="Invalid value.*for cgroup_path"):
        ResourceIsolationConfig(enable_resource_isolation=True, cgroup_path=1.0)


def test_enabled_resource_isolation_with_non_number_reserved_cpu_raises_value_error():

    with pytest.raises(ValueError, match="Invalid value.*for system_reserved_cpu."):
        ResourceIsolationConfig(
            enable_resource_isolation=True,
            system_reserved_cpu="1",
        )


def test_enabled_resource_isolation_with_non_number_reserved_memory_raises_value_error():

    with pytest.raises(ValueError, match="Invalid value.*for system_reserved_memory."):
        ResourceIsolationConfig(
            enable_resource_isolation=True,
            system_reserved_memory="1",
        )


def test_enabled_default_config_with_insufficient_cpu_and_memory_raises_value_error(
    monkeypatch,
):
    # The following values in ray_constants define the minimum requirements for resource isolation
    # 1) DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES
    # 2) DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES
    # NOTE: if you change the DEFAULT_MIN_SYSTEM_* constants, you may need to modify this test.
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 0.5)
    with pytest.raises(
        ValueError, match="available number of cpu cores.*less than the minimum"
    ):
        ResourceIsolationConfig(enable_resource_isolation=True)

    monkeypatch.undo()

    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 400 * (1024**2)
    )
    with pytest.raises(ValueError, match="available memory.*less than the minimum"):
        ResourceIsolationConfig(enable_resource_isolation=True)


def test_enabled_resource_isolation_with_default_config_picks_min_values(monkeypatch):
    # The following values in ray_constants define the minimum requirements for resource isolation
    # 1) DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES
    # 2) DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES
    # NOTE: if you change the DEFAULT_MIN_SYSTEM_* constants, you may need to modify this test.
    # if the total number of cpus is between [1,19] the system cgroup will a weight that is equal to 1 cpu core.
    # if the total amount of memory is between [0.5GB, 4.8GB] the system cgroup will get 0.5GB + object store memory.
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 2)
    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 1024**3
    )
    config = ResourceIsolationConfig(enable_resource_isolation=True)
    assert config.system_reserved_cpu_weight == 5000
    assert config.system_reserved_memory == 500 * (1024**2)

    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 19)
    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 4.8 * (1024**3)
    )
    config = ResourceIsolationConfig(enable_resource_isolation=True)
    assert config.system_reserved_cpu_weight == 526
    assert config.system_reserved_memory == 500 * (1024**2)


def test_enabled_resource_isolation_with_default_config_values_scale_with_system(
    monkeypatch,
):
    # The following values in ray_constants define the default proportion for resource isolation
    # 1) DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION
    # 2) DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION
    # NOTE: if you change the DEFAULT_SYSTEM_RESERVED_* constants, you may need to modify this test.
    # if the number of cpus on the system is [20,60] the reserved cpu cores will scale proportionately.
    # if the amount of memory on the system is [5GB, 100GB] the reserved system memory will scale proportionately.
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 20)
    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 5 * (1024**3)
    )
    config = ResourceIsolationConfig(enable_resource_isolation=True)
    assert config.system_reserved_cpu_weight == 500
    assert config.system_reserved_memory == 512 * (1024**2)

    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 59)
    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 99 * (1024**3)
    )
    config = ResourceIsolationConfig(enable_resource_isolation=True)
    assert config.system_reserved_cpu_weight == 500
    assert config.system_reserved_memory == 10630044057  # 9.9GiB


def test_enabled_resource_isolation_with_default_config_picks_max_values(monkeypatch):
    # The following values in ray_constants define the max reserved values for resource isolation
    # 1) DEFAULT_MAX_SYSTEM_RESERVED_CPU_CORES
    # 2) DEFAULT_MAX_SYSTEM_RESERVED_MEMORY_BYTES
    # NOTE: if you change the DEFAULT_MAX_SYSTEM* constants, you may need to modify this test.
    # if the number of cpus on the system >= 60 the reserved cpu cores will be DEFAULT_MAX_SYSTEM_RESERVED_CPU_CORES.
    # if the amount of memory on the system >= 100GB the reserved memory will be DEFAULT_MAX_SYSTEM_RESERVED_MEMORY_BYTES.
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 61)
    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 100 * (1024**3)
    )
    config = ResourceIsolationConfig(enable_resource_isolation=True)
    assert config.system_reserved_cpu_weight == 491
    assert config.system_reserved_memory == 10 * (1024**3)

    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 128)
    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 500 * (1024**3)
    )
    config = ResourceIsolationConfig(enable_resource_isolation=True)
    assert config.system_reserved_cpu_weight == 234
    assert config.system_reserved_memory == 10 * (1024**3)


def test_enabled_with_resource_overrides_less_than_minimum_defaults_raise_value_error():
    # The following values in ray_constants define the min values needed to run ray with resource isolation.
    # 1) DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES
    # 2) DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES
    # NOTE: if you change the DEFAULT_MIN_SYSTEM* constants, you may need to modify this test.
    with pytest.raises(
        ValueError,
        match="The requested system_reserved_cpu=0.5 is less than the minimum number of cpus that can be used for resource isolation.",
    ):
        ResourceIsolationConfig(
            enable_resource_isolation=True,
            system_reserved_cpu=0.5,
        )

    with pytest.raises(
        ValueError,
        match="The requested system_reserved_cpu=0.0 is less than the minimum number of cpus that can be used for resource isolation.",
    ):
        ResourceIsolationConfig(
            enable_resource_isolation=True,
            system_reserved_cpu=0,
        )

    with pytest.raises(
        ValueError,
        match="The requested system_reserved_memory 4194304 is less than the minimum number of bytes that can be used for resource isolation.",
    ):
        ResourceIsolationConfig(
            enable_resource_isolation=True,
            system_reserved_memory=4 * (1024**2),
        )

    with pytest.raises(
        ValueError,
        match="The requested system_reserved_memory 0 is less than the minimum number of bytes that can be used for resource isolation.",
    ):
        ResourceIsolationConfig(
            enable_resource_isolation=True,
            system_reserved_memory=0,
        )


def test_enabled_with_resource_overrides_gte_than_available_resources_raise_value_error(
    monkeypatch,
):
    # The following values in ray_constants define the maximum reserved values to run ray with resource isolation.
    # 1) DEFAULT_MAX_SYSTEM_RESERVED_CPU_CORES
    # 2) DEFAULT_MAX_SYSTEM_RESERVED_MEMORY_BYTES
    # NOTE: if you change the DEFAULT_MAX_SYSTEM* constants, you may need to modify this test.
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 32)
    with pytest.raises(
        ValueError,
        match="The requested system_reserved_cpu=32.0 is greater than or equal to the number of cpus available=32",
    ):
        ResourceIsolationConfig(
            enable_resource_isolation=True,
            system_reserved_cpu=32,
        )

    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 10 * (1024**3)
    )
    # 11GiB requested, 10GB available
    with pytest.raises(
        ValueError,
        match=r"The total requested system_reserved_memory=11811160064 is greater than the amount of memory available=10737418240\.",
    ):
        ResourceIsolationConfig(
            enable_resource_isolation=True,
            system_reserved_memory=11 * (1024**3),
        )


def test_resource_isolation_enabled_with_partial_resource_overrides_and_defaults_happy_path(
    monkeypatch,
):
    # This is a happy path test where all overrides are specified with valid values.
    # NOTE: if you change the DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION, this test may fail.
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 32)
    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 64 * (1024**3)
    )

    # Overriding cgroup_path while using default system_reserved_cpu and system_reserved_memory
    override_cgroup_path_config: ResourceIsolationConfig = ResourceIsolationConfig(
        enable_resource_isolation=True,
        cgroup_path="/sys/fs/cgroup/ray",
    )
    assert override_cgroup_path_config.cgroup_path == "/sys/fs/cgroup/ray"
    # (32 cpus * 0.05 (default))/10000 = 500
    assert override_cgroup_path_config.system_reserved_cpu_weight == 500
    # 64GB * 0.10 = 6.4GB
    assert override_cgroup_path_config.system_reserved_memory == 6871947673  # 6.4GiB

    # Overriding system_reserved_cpu while using default cgroup_path and system_reserved_memory
    override_cpu_config: ResourceIsolationConfig = ResourceIsolationConfig(
        enable_resource_isolation=True, system_reserved_cpu=1.5
    )
    assert override_cpu_config.system_reserved_cpu_weight == 468
    # defaults to /sys/fs/cgroup
    assert override_cpu_config.cgroup_path == "/sys/fs/cgroup"
    # 64GB * 0.10 = 6.4GB
    assert override_cpu_config.system_reserved_memory == 6871947673  # 6.4GiB

    # Overriding system_reserved_memory while using default cgroup_path and system_reserved_cpu
    override_memory_config: ResourceIsolationConfig = ResourceIsolationConfig(
        enable_resource_isolation=True,
        system_reserved_memory=5 * (1024**3),
    )
    assert override_memory_config.system_reserved_memory == 5368709120  # 5GiB
    # defaults to /sys/fs/cgroup
    assert override_memory_config.cgroup_path == "/sys/fs/cgroup"
    # (32 cpus * 0.05 (default))/10000 = 500
    assert override_memory_config.system_reserved_cpu_weight == 500


def test_resource_isolation_enabled_with_full_overrides_happy_path(monkeypatch):
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 32)
    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 128 * (1024**3)
    )
    # The system_reserved_cpu is deliberately > the maximum default.
    # The system_reserved_memory is deliberately > the maximum default.
    override_config: ResourceIsolationConfig = ResourceIsolationConfig(
        enable_resource_isolation=True,
        cgroup_path="/sys/fs/cgroup/ray",
        system_reserved_cpu=5.0,
        system_reserved_memory=15 * 1024**3,
    )

    assert override_config.cgroup_path == "/sys/fs/cgroup/ray"
    # int(5/32 * 10000)
    assert override_config.system_reserved_cpu_weight == 1562
    assert override_config.system_reserved_memory == 15 * (1024**3)


def test_system_processes_omit_pids_that_no_longer_exist():
    """The raylet dies if it cannot move a pid into the system cgroup.

    A system process is allowed to exit before the raylet starts, so its pid must not
    reach the raylet.
    """
    exited = subprocess.Popen([sys.executable, "-c", ""])
    exited.wait()

    node = SimpleNamespace(all_processes=_all_processes(os.getpid(), exited.pid))
    pids = Node._get_system_processes_for_resource_isolation(node)

    assert str(exited.pid) not in pids.split(",")
    assert str(os.getpid()) in pids.split(",")


@pytest.mark.parametrize("error", [psutil.NoSuchProcess, psutil.AccessDenied])
def test_system_processes_collected_when_dashboard_is_unreadable(monkeypatch, error):
    """An api server that cannot be inspected must not fail node startup.

    The api server runs with raise_on_api_server_failure=False by default, so it can be
    gone by the time its descendants are enumerated. psutil then raises NoSuchProcess,
    or AccessDenied on platforms that restrict inspecting other processes.
    """

    def raise_error(pid):
        raise error(pid)

    monkeypatch.setattr(psutil, "Process", raise_error)
    # Real pids: dead ones are filtered out before the raylet sees them.
    gcs_pid, dashboard_pid = os.getpid(), os.getppid()

    node = SimpleNamespace(all_processes=_all_processes(gcs_pid, dashboard_pid))
    pids = Node._get_system_processes_for_resource_isolation(node)

    assert pids.split(",") == [str(gcs_pid), str(dashboard_pid)]


_SLEEP = "import time; time.sleep(60)"
_SPAWN_AND_SLEEP = (
    "import subprocess, sys, time; "
    "subprocess.Popen([sys.executable, '-c', {inner!r}]); "
    "time.sleep(60)"
)


def test_system_processes_include_grandchildren_of_the_dashboard():
    """Subprocess modules are grandchildren of the api server, not children.

    Under the forkserver start method the api server's only child is the forkserver and
    the modules hang off that, so a non-recursive lookup collects none of them.
    """
    middle = _SPAWN_AND_SLEEP.format(inner=_SLEEP)
    outer = _SPAWN_AND_SLEEP.format(inner=middle)
    dashboard = subprocess.Popen([sys.executable, "-c", outer])

    def grandchildren_of_dashboard():
        parent = psutil.Process(dashboard.pid)
        return [
            grandchild for child in parent.children() for grandchild in child.children()
        ]

    try:
        deadline = time.monotonic() + 15
        while not grandchildren_of_dashboard() and time.monotonic() < deadline:
            time.sleep(0.05)
        found = grandchildren_of_dashboard()
        assert found, "the test never built a two-level process tree"
        grandchild_pid = found[0].pid

        node = SimpleNamespace(all_processes=_all_processes(os.getpid(), dashboard.pid))
        pids = Node._get_system_processes_for_resource_isolation(node)

        assert str(grandchild_pid) in pids.split(",")
    finally:
        # The tree is gone already if the test failed early; that must not mask the
        # original failure.
        with contextlib.suppress(psutil.Error):
            for process in psutil.Process(dashboard.pid).children(recursive=True):
                process.kill()
        dashboard.kill()
        dashboard.wait()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
