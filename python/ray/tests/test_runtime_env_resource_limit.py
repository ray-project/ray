# coding: utf-8
import os
import sys

import pytest

import ray
from ray.workers import setup_cgroup


@pytest.mark.skipif(
    sys.platform != "linux", reason="cgroup only supported on linux.")
def test_resource_limit_without_container(shutdown_only):
    ray.init(_system_config={"worker_resource_limits_enabled": True})

    def _get_memory_cgroup_name():
        for line in open("/proc/self/cgroup"):
            cgroup_info = line.split(":")
            if cgroup_info[1] == "memory":
                return os.path.basename(cgroup_info[2].rstrip())
        return ""

    def _get_cpu_cgroup_name():
        for line in open("/proc/self/cgroup"):
            cgroup_info = line.split(":")
            for cgroup_name in cgroup_info[1].split(","):
                if cgroup_name == "cpu":
                    return os.path.basename(cgroup_info[2].rstrip())
        return ""

    @ray.remote
    def get_memory_cgroup_name():
        return _get_memory_cgroup_name()

    @ray.remote
    def get_cpu_cgroup_name():
        return _get_cpu_cgroup_name()

    @ray.remote
    def get_memory_cgroup_value():
        cgroup_name = _get_memory_cgroup_name()
        cgroup_path = os.path.join(setup_cgroup.cgroup_v1_root, "memory",
                                   setup_cgroup.ray_parent_cgroup_name,
                                   cgroup_name)
        return setup_cgroup.get_ray_cgroup_property(cgroup_path,
                                                    "memory.limit_in_bytes")

    @ray.remote
    def get_cpu_cgroup_value():
        cgroup_name = _get_cpu_cgroup_name()
        cgroup_path = os.path.join(setup_cgroup.cgroup_v1_root, "cpu",
                                   setup_cgroup.ray_parent_cgroup_name,
                                   cgroup_name)
        return setup_cgroup.get_ray_cgroup_property(cgroup_path, "cpu.shares")

    memory_cgroup_name = ray.get(
        get_memory_cgroup_name.options(
            memory=100 * 1024 * 1024, runtime_env={
                "env_vars": {
                    "a": "b",
                }
            }).remote())
    assert memory_cgroup_name.startswith("ray-")

    cpu_cgroup_name = ray.get(
        get_cpu_cgroup_name.options(
            num_cpus=1, runtime_env={
                "env_vars": {
                    "a": "b",
                }
            }).remote())
    assert cpu_cgroup_name.startswith("ray-")

    memory_value = ray.get(
        get_memory_cgroup_value.options(
            memory=100 * 1024 * 1024, runtime_env={
                "env_vars": {
                    "a": "b",
                }
            }).remote())
    assert memory_value == str(100 * 1024 * 1024)

    cpu_value = ray.get(
        get_cpu_cgroup_value.options(
            num_cpus=1, runtime_env={
                "env_vars": {
                    "a": "b",
                }
            }).remote())
    assert cpu_value == str(1024)


@pytest.mark.skipif(
    sys.platform != "linux", reason="cgroup only supported on linux.")
def test_cpuset_resource_limit_without_container(shutdown_only):
    ray.init(
        _system_config={
            "worker_resource_limits_enabled": True,
            "predefined_unit_instance_resources": "CPU,GPU"
        })

    def _get_cpuset_cgroup_name():
        for line in open("/proc/self/cgroup"):
            cgroup_info = line.split(":")
            if cgroup_info[1] == "cpuset":
                return os.path.basename(cgroup_info[2].rstrip())
        return ""

    @ray.remote
    def get_cpuset_cgroup_name():
        return _get_cpuset_cgroup_name()

    @ray.remote
    def get_cpuset_cgroup_value():
        cgroup_name = _get_cpuset_cgroup_name()
        cgroup_path = os.path.join(setup_cgroup.cgroup_v1_root, "cpuset",
                                   setup_cgroup.ray_parent_cgroup_name,
                                   cgroup_name)
        return setup_cgroup.get_ray_cgroup_property(cgroup_path, "cpuset.cpus")

    cpuset_cgroup_name = ray.get(
        get_cpuset_cgroup_name.options(
            num_cpus=1, runtime_env={
                "env_vars": {
                    "a": "b",
                }
            }).remote())
    assert cpuset_cgroup_name.startswith("ray-")

    cpuset_value = ray.get(
        get_cpuset_cgroup_value.options(
            num_cpus=1, runtime_env={
                "env_vars": {
                    "a": "b",
                }
            }).remote())
    assert cpuset_value == "0"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__, "-s"]))
