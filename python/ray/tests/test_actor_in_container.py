import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import sys

import ray
import ray.job_config
import ray.test_utils
from ray.tests.conftest import shutdown_only  # noqa: F401
import ray.cluster_utils


@pytest.mark.skipif("sys.platform != 'linux'")
def test_actor_in_container(shutdown_only):
    container_image = "rayproject/ray-nest-container:nightly-py36-cpu"
    ray.init(
        job_config=ray.job_config.JobConfig(
            worker_container_image=container_image),
        _system_config={
            "worker_process_in_container_enabled": True,
        })

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


# Raylet runs in container image "ray-nest-container:nightly-py36-cpu"
# Ray job run in container image "ray-nest-container:nightly-py36-cpu-pandas"
@pytest.mark.skipif("sys.platform != 'linux'")
def test_actor_in_heterogeneous_image(shutdown_only):
    container_image = "rayproject/ray-nest-container:nightly-py36-cpu-pandas"
    ray.init(
        job_config=ray.job_config.JobConfig(
            worker_container_image=container_image),
        _system_config={
            "worker_process_in_container_enabled": True,
        })

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


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
