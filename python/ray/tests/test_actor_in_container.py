import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import sys

import ray
import ray.job_config
import ray.test_utils
import ray.cluster_utils


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_actor_in_container():
    job_config = ray.job_config.JobConfig(
        runtime_env={
            "container": {
                "image": "rayproject/ray-worker-container:nightly-py36-cpu",
            }
        })
    ray.init(job_config=job_config)

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

        def get_counter(self):
            return self.value

    a1 = Counter.options().remote()
    a1.increment.remote()
    result = ray.get(a1.get_counter.remote())
    assert result == 1
    ray.shutdown()


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_actor_in_heterogeneous_image():
    job_config = ray.job_config.JobConfig(
        runtime_env={
            "container": {
                "image": "rayproject/ray-worker-container:"
                "nightly-py36-cpu-pandas",
            }
        })
    ray.init(job_config=job_config)

    @ray.remote
    class HeterogeneousActor(object):
        def __init__(self):
            pass

        def run_pandas(self):
            import numpy as np
            import pandas as pd
            return len(pd.Series([1, 3, 5, np.nan, 6]))

    h1 = HeterogeneousActor.options().remote()
    pandas_result = ray.get(h1.run_pandas.remote())
    assert pandas_result == 5
    ray.shutdown()


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_actor_in_container_with_resource_limit():
    job_config = ray.job_config.JobConfig(
        runtime_env={
            "container": {
                "image": "rayproject/ray-worker-container:nightly-py36-cpu",
                # Currently runc will mount individual cgroupfs
                # issue: https://github.com/containers/podman/issues/10989
                "run_options": [
                    "--runtime", "/usr/sbin/runc", "--security-opt",
                    "seccomp=unconfined"
                ],
            }
        })
    ray.init(
        job_config=job_config,
        _system_config={"worker_resource_limits_enabled": True})

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.value = 0

        def get_memory_limit(self):
            f = open("/sys/fs/cgroup/memory/memory.limit_in_bytes", "r")
            return f.readline().strip("\n")

        def get_cpu_share(self):
            f = open("/sys/fs/cgroup/cpu/cpu.shares", "r")
            return f.readline().strip("\n")

    a1 = Counter.options(num_cpus=2, memory=100 * 1024 * 1024).remote()
    cpu_share = ray.get(a1.get_cpu_share.remote())
    assert cpu_share == "2048"
    memory_limit = ray.get(a1.get_memory_limit.remote())
    assert memory_limit == "104857600"
    ray.shutdown()


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_reuse_worker_with_resource_limit():
    runtime_env = {
        "container": {
            "image": "rayproject/ray-worker-container:nightly-py36-cpu",
        }
    }
    job_config = ray.job_config.JobConfig()
    ray.init(
        job_config=job_config,
        _system_config={"worker_resource_limits_enabled": True})

    @ray.remote
    def get_pid():
        import os
        return os.getpid()

    obj_ref = get_pid.options(
        runtime_env=runtime_env, num_cpus=1,
        memory=100 * 1024 * 1024).remote()
    pid = ray.get(obj_ref)
    obj_ref1 = get_pid.options(
        runtime_env=runtime_env, num_cpus=1,
        memory=100 * 1024 * 1024).remote()
    pid1 = ray.get(obj_ref1)
    assert pid == pid1
    obj_ref2 = get_pid.options(
        runtime_env=runtime_env, num_cpus=1, memory=50 * 1024 * 1024).remote()
    pid2 = ray.get(obj_ref2)
    assert pid != pid2
    ray.shutdown()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__, "-s"]))
