import sys

import pytest

from ray._private import utils
from ray._private.resource_isolation_config import ResourceIsolationConfig


def test_disabled_by_default():
    resource_isolation_config = ResourceIsolationConfig()
    assert not resource_isolation_config.is_enabled()


def test_disabled_isolation_with_cgroup_path_raises_exception():
    with pytest.raises(ValueError):
        ResourceIsolationConfig(
            enable_resource_isolation=False, cgroup_path="/some/path"
        )


def test_disabled_isolation_with_reserved_cpu_raises_exception():
    with pytest.raises(ValueError):
        ResourceIsolationConfig(enable_resource_isolation=False, system_reserved_cpu=1)


def test_disabled_isolation_with_reserved_memory_raises_exception():
    with pytest.raises(ValueError):
        ResourceIsolationConfig(
            enable_resource_isolation=False, system_reserved_memory=1
        )


def test_enabled_invalid_cgroup_path_type():
    with pytest.raises(ValueError):
        ResourceIsolationConfig(enable_resource_isolation=True, cgroup_path=1)


def test_enabled_invalid_reserved_cpu_type():
    with pytest.raises(ValueError):
        ResourceIsolationConfig(enable_resource_isolation=True, system_reserved_cpu="1")


def test_enabled_invalid_reserved_memory_type():
    with pytest.raises(ValueError):
        ResourceIsolationConfig(enable_resource_isolation=True, system_reserved_cpu="1")


def test_enabled_default_config_proportions(monkeypatch):
    object_store_memory = 10 * 10**9
    total_system_memory = 128 * 10**9
    total_system_cpu = 32
    monkeypatch.setattr(
        "ray._common.utils.get_system_memory",
        lambda *args, **kwargs: total_system_memory,
    )
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: total_system_cpu)
    resource_isolation_config = ResourceIsolationConfig(enable_resource_isolation=True)
    resource_isolation_config.add_object_store_memory(object_store_memory)
    # expect the default to be the min(128 * 0.10, 25G) + object_store_memory
    expected_reserved_memory = 22800000000
    # expect the default to be the min(32 * 0.05, 1)/32 * 10000
    expected_reserved_cpu_weight = 312
    assert resource_isolation_config.system_reserved_memory == expected_reserved_memory
    assert (
        resource_isolation_config.system_reserved_cpu_weight
        == expected_reserved_cpu_weight
    )


def test_enabled_default_config_values(monkeypatch):
    object_store_memory = 10 * 10**9
    total_system_memory = 500 * 10**9
    total_system_cpu = 64
    monkeypatch.setattr(
        "ray._common.utils.get_system_memory",
        lambda *args, **kwargs: total_system_memory,
    )
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: total_system_cpu)
    resource_isolation_config = ResourceIsolationConfig(enable_resource_isolation=True)
    resource_isolation_config.add_object_store_memory(object_store_memory)
    # expect the default to be the min(500 * 0.10, 25G) + object_store_memory
    expected_reserved_memory = 35000000000
    # expect the default to be the min(64 * 0.05, 1)/64 * 10000
    expected_reserved_cpu_weight = 156
    assert resource_isolation_config.system_reserved_memory == expected_reserved_memory
    assert (
        resource_isolation_config.system_reserved_cpu_weight
        == expected_reserved_cpu_weight
    )


def test_enabled_reserved_cpu_default_memory(monkeypatch):
    object_store_memory = 10 * 10**9
    total_system_memory = 128 * 10**9
    total_system_cpu = 32
    system_reserved_cpu = 5
    monkeypatch.setattr(
        "ray._common.utils.get_system_memory",
        lambda *args, **kwargs: total_system_memory,
    )
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: total_system_cpu)
    resource_isolation_config = ResourceIsolationConfig(
        enable_resource_isolation=True, system_reserved_cpu=system_reserved_cpu
    )
    resource_isolation_config.add_object_store_memory(object_store_memory)
    # expect the default to be the min(128 * 0.10, 25G) + object_store_memory
    expected_reserved_memory = 22800000000
    # expect the default to be the 5/32 * 10000
    expected_reserved_cpu_weight = 1562
    assert resource_isolation_config.system_reserved_memory == expected_reserved_memory
    assert (
        resource_isolation_config.system_reserved_cpu_weight
        == expected_reserved_cpu_weight
    )


def test_enabled_reserved_memory_default_cpu(monkeypatch):
    object_store_memory = 10 * 10**9
    total_system_memory = 128 * 10**9
    total_system_cpu = 32
    system_reserved_memory = 15 * 10**9
    monkeypatch.setattr(
        "ray._common.utils.get_system_memory",
        lambda *args, **kwargs: total_system_memory,
    )
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: total_system_cpu)
    resource_isolation_config = ResourceIsolationConfig(
        enable_resource_isolation=True, system_reserved_memory=system_reserved_memory
    )
    resource_isolation_config.add_object_store_memory(object_store_memory)
    # expect the default to be the min(128 * 0.10, 25G) + object_store_memory
    expected_reserved_memory = system_reserved_memory + object_store_memory
    # expect the default to be the min(32 * 0.05, 1)/32 * 1000
    expected_reserved_cpu_weight = 312
    assert resource_isolation_config.system_reserved_memory == expected_reserved_memory
    assert (
        resource_isolation_config.system_reserved_cpu_weight
        == expected_reserved_cpu_weight
    )


def test_enabled_override_all_default_values(monkeypatch):
    object_store_memory = 10 * 10**9
    total_system_memory = 128 * 10**9
    system_reserved_memory = 15 * 10**9
    total_system_cpu = 32
    system_reserved_cpu = 5
    cgroup_path = "/sys/fs/cgroup/subcgroup"
    monkeypatch.setattr(
        "ray._common.utils.get_system_memory",
        lambda *args, **kwargs: total_system_memory,
    )
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: total_system_cpu)
    resource_isolation_config = ResourceIsolationConfig(
        enable_resource_isolation=True,
        cgroup_path=cgroup_path,
        system_reserved_cpu=system_reserved_cpu,
        system_reserved_memory=system_reserved_memory,
    )
    resource_isolation_config.add_object_store_memory(object_store_memory)
    expected_reserved_memory = 25000000000
    expected_reserved_cpu_weight = 1562
    assert resource_isolation_config.system_reserved_memory == expected_reserved_memory
    assert (
        resource_isolation_config.system_reserved_cpu_weight
        == expected_reserved_cpu_weight
    )
    assert resource_isolation_config.cgroup_path == cgroup_path


def test_enabled_reserved_cpu_exceeds_available_cpu_raises_exception(monkeypatch):
    total_system_cpu = 32
    system_reserved_cpu = 33
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: total_system_cpu)
    with pytest.raises(ValueError):
        ResourceIsolationConfig(
            enable_resource_isolation=True, system_reserved_cpu=system_reserved_cpu
        )


def test_enabled_reserved_cpu_less_than_minimum_raises_exception(monkeypatch):
    system_reserved_cpu = 0.1
    with pytest.raises(ValueError):
        ResourceIsolationConfig(
            enable_resource_isolation=True, system_reserved_cpu=system_reserved_cpu
        )


def test_enabled_reserved_memory_exceeds_available_memory_raises_exception(monkeypatch):
    total_system_cpu = 32
    total_system_memory = 128 * 10**9
    system_reserved_memory = (128 * 10**9) + 1
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: total_system_cpu)
    monkeypatch.setattr(
        "ray._common.utils.get_system_memory",
        lambda *args, **kwargs: total_system_memory,
    )
    with pytest.raises(ValueError):
        ResourceIsolationConfig(
            enable_resource_isolation=True,
            system_reserved_memory=system_reserved_memory,
        )


def test_enabled_total_system_reserved_memory_exceeds_available_memory_raises_exception(
    monkeypatch,
):
    total_system_cpu = 32
    object_store_memory = 10 * 10**9
    total_system_memory = 128 * 10**9
    # combined with object store, it exceeds available memory
    system_reserved_memory = 119 * 10**9
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: total_system_cpu)
    monkeypatch.setattr(
        "ray._common.utils.get_system_memory",
        lambda *args, **kwargs: total_system_memory,
    )
    resource_isolation_config = ResourceIsolationConfig(
        enable_resource_isolation=True, system_reserved_memory=system_reserved_memory
    )
    with pytest.raises(ValueError):
        resource_isolation_config.add_object_store_memory(object_store_memory)


def test_enabled_reserved_memory_less_than_minimum_raises_exception(monkeypatch):
    system_reserved_memory = 1 * 10**3
    with pytest.raises(ValueError):
        ResourceIsolationConfig(
            enable_resource_isolation=True,
            system_reserved_memory=system_reserved_memory,
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
