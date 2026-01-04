import sys

import pytest

from ray._common import utils as common_utils
from ray._private import utils
from ray._private.resource_isolation_config import ResourceIsolationConfig


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
        match="system_reserved_memory cannot be set when resource isolation is not enabled",
    ):
        ResourceIsolationConfig(
            enable_resource_isolation=False, system_reserved_memory=1024**3
        )


def test_enabled_resource_isolation_with_non_string_cgroup_path_raises_value_error():

    with pytest.raises(ValueError, match="Invalid value.*for cgroup_path"):
        ResourceIsolationConfig(enable_resource_isolation=True, cgroup_path=1)

    with pytest.raises(ValueError, match="Invalid value.*for cgroup_path"):
        ResourceIsolationConfig(enable_resource_isolation=True, cgroup_path=1.0)


def test_enabled_resource_isolation_with_non_number_reserved_cpu_raises_value_error():

    with pytest.raises(ValueError, match="Invalid value.*for system_reserved_cpu."):
        ResourceIsolationConfig(enable_resource_isolation=True, system_reserved_cpu="1")


def test_enabled_resource_isolation_with_non_number_reserved_memory_raises_value_error():

    with pytest.raises(ValueError, match="Invalid value.*for system_reserved_memory."):
        ResourceIsolationConfig(
            enable_resource_isolation=True, system_reserved_memory="1"
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
        common_utils, "get_system_memory", lambda *args, **kwargs: 0.5 * (1024**3)
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
    assert config.system_reserved_memory == 536870912

    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 59)
    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 99 * (1024**3)
    )
    config = ResourceIsolationConfig(enable_resource_isolation=True)
    assert config.system_reserved_cpu_weight == 500
    assert config.system_reserved_memory == 10630044057


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
        ResourceIsolationConfig(enable_resource_isolation=True, system_reserved_cpu=0.5)

    with pytest.raises(
        ValueError,
        match="The requested system_reserved_memory 4194304 is less than the minimum number of bytes that can be used for resource isolation.",
    ):
        ResourceIsolationConfig(
            enable_resource_isolation=True, system_reserved_memory=4 * (1024**2)
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
        ResourceIsolationConfig(enable_resource_isolation=True, system_reserved_cpu=32)

    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 10 * (1024**3)
    )
    with pytest.raises(
        ValueError,
        match="The total requested system_reserved_memory=11811160064 is greater than the amount of memory available=10737418240",
    ):
        ResourceIsolationConfig(
            enable_resource_isolation=True, system_reserved_memory=11 * (1024**3)
        )


def test_add_object_store_memory_called_more_than_once_raises_value_error(monkeypatch):
    # Monkeypatch to make sure the underlying system's resources don't cause the test to fail.
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 32)
    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 128 * (1024**3)
    )
    config: ResourceIsolationConfig = ResourceIsolationConfig(
        enable_resource_isolation=True
    )
    config.add_object_store_memory(5 * (1024**3))
    with pytest.raises(
        AssertionError,
        match="Cannot call add_object_store_memory more than once with an instance ResourceIsolationConfig. This is a bug in the ray code",
    ):
        config.add_object_store_memory(5 * (1024**3))


def test_add_object_store_memory_plus_system_reserved_memory_gt_available_memory_raises_value_error(
    monkeypatch,
):
    # Monkeypatch to make sure the underlying system's resources don't cause the test to fail.
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 16)
    # 32GB of total memory available on the system.
    monkeypatch.setattr(
        common_utils, "get_system_memory", lambda *args, **kwargs: 32 * (1024**3)
    )
    # 16GB reserved for system processes.
    config: ResourceIsolationConfig = ResourceIsolationConfig(
        enable_resource_isolation=True, system_reserved_memory=16 * (1024**3)
    )
    # 16GB + 1 byte reserved for object store.
    with pytest.raises(
        ValueError,
        match=r"The total requested system_reserved_memory=34359738369.*is greater than the total memory available=34359738368",
    ):
        config.add_object_store_memory(16 * (1024**3) + 1)


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
        enable_resource_isolation=True, cgroup_path="/sys/fs/cgroup/ray"
    )
    assert override_cgroup_path_config.cgroup_path == "/sys/fs/cgroup/ray"
    # (32 cpus * 0.05 (default))/10000 = 500
    assert override_cgroup_path_config.system_reserved_cpu_weight == 500
    # 64GB * 0.10 = 6.4GB
    assert override_cgroup_path_config.system_reserved_memory == 6871947673

    # Overriding system_reserved_cpu while using default cgroup_path and system_reserved_memory
    override_cpu_config: ResourceIsolationConfig = ResourceIsolationConfig(
        enable_resource_isolation=True, system_reserved_cpu=1.5
    )
    assert override_cpu_config.system_reserved_cpu_weight == 468
    # defaults to /sys/fs/cgroup
    assert override_cpu_config.cgroup_path == "/sys/fs/cgroup"
    # 64GB * 0.10 = 6.4GB
    assert override_cpu_config.system_reserved_memory == 6871947673

    # Overriding system_reserved_memory while using default cgroup_path and system_reserved_cpu
    override_memory_config: ResourceIsolationConfig = ResourceIsolationConfig(
        enable_resource_isolation=True, system_reserved_memory=5 * (1024**3)
    )
    assert override_memory_config.system_reserved_memory == 5368709120
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
    # Adding the 38G of object store memory.
    override_config.add_object_store_memory(38 * (1024**3))

    assert override_config.cgroup_path == "/sys/fs/cgroup/ray"
    # int(5/32 * 10000)
    assert override_config.system_reserved_cpu_weight == 1562
    # system_reserved_memory + object_store_memory = 15G + 38G = 53G
    assert override_config.system_reserved_memory == 53 * (1024**3)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
