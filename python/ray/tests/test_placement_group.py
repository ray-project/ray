import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import sys
import os
import time

import ray
import ray.test_utils
import ray.cluster_utils


@pytest.mark.skipif(
    os.environ.get("RAY_GCS_ACTOR_SERVICE_ENABLED") != "true",
    reason=("This edge case is not handled when GCS actor management is off. "
            "We won't fix this because GCS actor management "
            "will be on by default anyway."))
@pytest.mark.parametrize(
    "ray_start_regular", [{
        "num_cpus": 4
    }], indirect=True)
def test_placement_group(ray_start_regular):
    @ray.remote
    class Counter(object):
        def __init__(self):
            self.n = 0

        def increment(self):
            self.n += 5

        def read(self):
            return self.n

    placement_group_id = ray.experimental.placement_group(name="name",
                                                          strategy="pack",
                                                          bundles=[{"CPU": 4}])
    print(placement_group_id)

    counters = [Counter.options(placement_group_id=placement_group_id,
                                bundle_index=0).remote() for _ in range(4)]
    [c.increment.remote() for c in counters]
    futures = [c.read.remote() for c in counters]
    print(ray.get(futures))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
