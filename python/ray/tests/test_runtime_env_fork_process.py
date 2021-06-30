# coding: utf-8
import os
import sys

import pytest

import ray


@pytest.mark.skipif(
    sys.platform == "win32", reason="Fork API is not supported on Windows")
def test_fork_process_in_runtime_env(ray_start_cluster):
    cluster = ray_start_cluster
    directory = os.path.dirname(os.path.realpath(__file__))
    setup_worker_path = os.path.join(directory, "mock_setup_worker.py")
    cluster.add_node(num_cpus=1, setup_worker_path=setup_worker_path)
    job_config = ray.job_config.JobConfig(
        runtime_env={"env_vars": {
            "a": "b",
        }})
    ray.init(address=cluster.address, job_config=job_config)

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.value = 0
            ray.put(self.value)

        def increment(self):
            self.value += 1
            return self.value

        def get_counter(self):
            return self.value

    a1 = Counter.options().remote()
    a1.increment.remote()
    obj_ref1 = a1.get_counter.remote()
    result = ray.get(obj_ref1)
    assert result == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
