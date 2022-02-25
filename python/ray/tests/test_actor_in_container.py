import pytest

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import sys

import ray
import ray.job_config
import ray.cluster_utils


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on linux.")
def test_actor_in_container():
    job_config = ray.job_config.JobConfig(
        runtime_env={
            "container": {
                "image": "rayproject/ray-worker-container:nightly-py36-cpu",
            }
        }
    )
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
                "image": "rayproject/ray-worker-container:" "nightly-py36-cpu-pandas",
            }
        }
    )
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


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__, "-s"]))
