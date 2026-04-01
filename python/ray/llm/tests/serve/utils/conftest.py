import pytest

import ray
from ray.cluster_utils import Cluster


@pytest.fixture
def ray_start_cluster():
    cluster = Cluster()
    yield cluster
    ray.shutdown()
    cluster.shutdown()
