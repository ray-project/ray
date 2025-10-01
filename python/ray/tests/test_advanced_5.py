# coding: utf-8
import logging
import sys

import numpy as np
import pytest

from ray._private.test_utils import client_test_enabled

if client_test_enabled():
    from ray.util.client import ray
else:
    import ray

logger = logging.getLogger(__name__)


def test_task_arguments_inline_bytes_limit(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    cluster.add_node(
        num_cpus=1,
        resources={"pin_head": 1},
        _system_config={
            "max_direct_call_object_size": 100 * 1024,
            # if task_rpc_inlined_bytes_limit is greater than
            # max_grpc_message_size, this test fails.
            "task_rpc_inlined_bytes_limit": 18 * 1024,
            "max_grpc_message_size": 20 * 1024,
        },
    )
    cluster.add_node(num_cpus=1, resources={"pin_worker": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"pin_worker": 1})
    def foo(ref1, ref2, ref3):
        return ref1 == ref2 + ref3

    @ray.remote(resources={"pin_head": 1})
    def bar():
        # if the refs are inlined, the test fails.
        # refs = [ray.put(np.random.rand(1024) for _ in range(3))]
        # return ray.get(
        #     foo.remote(refs[0], refs[1], refs[2]))

        return ray.get(
            foo.remote(
                np.random.rand(1024),  # 8k
                np.random.rand(1024),  # 8k
                np.random.rand(1024),
            )
        )  # 8k

    ray.get(bar.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
