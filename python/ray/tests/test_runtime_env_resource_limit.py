# coding: utf-8
import os
import sys

import cgroupspy.trees, cgroupspy.controllers
import pytest

import ray


@pytest.mark.skipif(
    sys.platform != "linux", reason="cgroup only supported on linux.")
def test_resource_limit_without_container(shutdown_only):
    ray.init(
        _system_config={"worker_resource_limits_enabled": True})

    def _get_memory_cgroup_name():
        for line in open("/proc/self/cgroup"):
            cgroup_info = line.split(":")
            if cgroup_info[1] == "memory":
                return cgroup_info[2].rstrip()
        return ""

    def _get_cpu_cgroup_name():
        for line in open("/proc/self/cgroup"):
            cgroup_info = line.split(":")
            for cgroup_name in cgroup_info[1].split(","):
                if cgroup_name == "cpu":
                    return cgroup_info[2].rstrip()
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
        t = cgroupspy.trees.Tree()
        memory_cset = t.get_node_by_path("/memory" + cgroup_name)
        ctl = cgroupspy.controllers.Controller(memory_cset)
        return ctl.get_property(b"memory.limit_in_bytes")

    @ray.remote
    def get_cpu_cgroup_value():
        cgroup_name = _get_cpu_cgroup_name()
        t = cgroupspy.trees.Tree()
        cpu_cset = t.get_node_by_path("/cpu" + cgroup_name)
        ctl = cgroupspy.controllers.Controller(cpu_cset)
        return ctl.get_property(b"cpu.shares")

    memory_cgroup_name = ray.get(
        get_memory_cgroup_name.options(memory=100 * 1024 * 1024,
                                       runtime_env={"env_vars": {
                                           "a": "b",
                                       }}).remote())
    assert memory_cgroup_name.startswith('/ray-')

    cpu_cgroup_name = ray.get(
        get_cpu_cgroup_name.options(num_cpus=1,
                                    runtime_env={"env_vars": {
                                        "a": "b",
                                    }}).remote())
    assert cpu_cgroup_name.startswith('/ray-')

    memory_value = ray.get(
        get_memory_cgroup_value.options(memory=100 * 1024 * 1024,
                                        runtime_env={"env_vars": {
                                            "a": "b",
                                        }}).remote())
    assert memory_value == str(100 * 1024 * 1024)

    cpu_value = ray.get(
        get_cpu_cgroup_value.options(num_cpus=1,
                                     runtime_env={"env_vars": {
                                         "a": "b",
                                     }}).remote())
    assert cpu_value == str(1024)


@pytest.mark.skipif(
    sys.platform != "linux", reason="cgroup only supported on linux.")
def test_cpuset_resource_limit_without_container(shutdown_only):
    ray.init(
        _system_config={"worker_resource_limits_enabled": True,
                        "predefined_unit_instance_resources": "CPU,GPU"})

    def _get_cpuset_cgroup_name():
        for line in open("/proc/self/cgroup"):
            cgroup_info = line.split(":")
            if cgroup_info[1] == "cpuset":
                return cgroup_info[2].rstrip()
        return ""

    @ray.remote
    def get_cpuset_cgroup_name():
        return _get_cpuset_cgroup_name()

    @ray.remote
    def get_cpuset_cgroup_value():
        cgroup_name = _get_cpuset_cgroup_name()
        t = cgroupspy.trees.Tree()
        cpu_cset = t.get_node_by_path("/cpuset" + cgroup_name)
        ctl = cgroupspy.controllers.Controller(cpu_cset)
        return ctl.get_property(b"cpuset.cpus")

    cpuset_cgroup_name = ray.get(
        get_cpuset_cgroup_name.options(num_cpus=1,
                                       runtime_env={"env_vars": {
                                           "a": "b",
                                       }}).remote())
    assert cpuset_cgroup_name.startswith('/ray-')

    cpuset_value = ray.get(
        get_cpuset_cgroup_value.options(num_cpus=1,
                                        runtime_env={"env_vars": {
                                            "a": "b",
                                        }}).remote())
    assert cpuset_value == "0"


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
