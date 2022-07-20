# coding: utf-8
import os
import sys
import json

import pytest

import ray
from ray._private.runtime_env.constants import RAY_JOB_CONFIG_JSON_ENV_VAR


@pytest.mark.skipif(
    sys.platform == "win32", reason="Fork API is not supported on Windows"
)
def test_fork_process_in_runtime_env(ray_start_cluster):
    cluster = ray_start_cluster
    directory = os.path.dirname(os.path.realpath(__file__))
    setup_worker_path = os.path.join(directory, "mock_setup_worker.py")
    cluster.add_node(num_cpus=1, setup_worker_path=setup_worker_path)
    job_config = ray.job_config.JobConfig(
        runtime_env={
            "env_vars": {
                "a": "b",
            }
        }
    )
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


@pytest.mark.skipif(
    sys.platform == "win32", reason="Fork API is not supported on Windows"
)
def test_fork_process_job_config_from_env_var(ray_start_cluster):

    os.environ[RAY_JOB_CONFIG_JSON_ENV_VAR] = json.dumps(
        {
            "runtime_env": {
                "env_vars": {
                    "a": "b",
                }
            }
        }
    )
    cluster = ray_start_cluster
    directory = os.path.dirname(os.path.realpath(__file__))
    setup_worker_path = os.path.join(directory, "mock_setup_worker.py")
    cluster.add_node(num_cpus=1, setup_worker_path=setup_worker_path)

    # Do not explicitly pass job config
    ray.init(address=cluster.address)

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

        def get_runtime_env(self):
            return ray.get_runtime_context().runtime_env

    a1 = Counter.options().remote()
    a1.increment.remote()
    obj_ref1 = a1.get_counter.remote()
    result = ray.get(obj_ref1)
    assert result == 1
    # Check runtime env setup
    assert ray.get(a1.get_runtime_env.remote()) == {"env_vars": {"a": "b"}}


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
