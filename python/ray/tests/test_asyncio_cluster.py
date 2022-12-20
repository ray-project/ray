# coding: utf-8
import asyncio
import sys

import pytest
import numpy as np

import ray
from ray.cluster_utils import Cluster, cluster_not_supported


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
@pytest.mark.asyncio
async def test_asyncio_cluster_wait():
    cluster = Cluster()
    head_node = cluster.add_node()
    cluster.add_node(resources={"OTHER_NODE": 100})

    ray.init(address=head_node.address)

    @ray.remote(num_cpus=0, resources={"OTHER_NODE": 1})
    def get_array():
        return np.random.random((192, 1080, 3)).astype(np.uint8)  # ~ 0.5MB

    object_ref = get_array.remote()

    await asyncio.wait_for(object_ref, timeout=10)

    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    import os
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
