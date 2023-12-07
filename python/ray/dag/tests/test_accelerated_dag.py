# coding: utf-8
import logging
import os
import sys

import pytest

import ray
import ray.cluster_utils
from ray.dag import InputNode, OutputNode
from ray.tests.conftest import *  # noqa


logger = logging.getLogger(__name__)


@ray.remote(concurrency_groups={"_ray_system": 1})
class Actor:
    def __init__(self, init_value):
        print("__init__ PID", os.getpid())
        self.i = init_value

    def inc(self, x):
        self.i += x
        return self.i


@pytest.mark.parametrize("num_actors", [1, 4])
def test_scatter_gather_dag(ray_start_regular, num_actors):
    init_val = 0
    actors = [Actor.remote(init_val) for _ in range(num_actors)]
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = OutputNode(out)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        output_channels = compiled_dag.execute(1)
        # TODO(swang): Replace with fake ObjectRef.
        results = [chan.begin_read() for chan in output_channels]
        assert results == [init_val + i + 1] * num_actors
        for chan in output_channels:
            chan.end_read()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
