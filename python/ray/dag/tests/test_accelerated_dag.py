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

    def inc_two(self, x, y):
        self.i += x
        self.i += y
        return self.i


def test_single_output_dag(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as i:
        dag = a.inc.bind(i)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        output_channel = compiled_dag.execute(1)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == i + 1
        output_channel.end_read()


def test_regular_args(ray_start_regular):
    # Test passing regular args to .bind in addition to DAGNode args.
    a = Actor.remote(0)
    with InputNode() as i:
        dag = a.inc_two.bind(2, i)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        output_channel = compiled_dag.execute(1)
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == (i + 1) * 3
        output_channel.end_read()


@pytest.mark.parametrize("num_actors", [1, 4])
def test_scatter_gather_dag(ray_start_regular, num_actors):
    actors = [Actor.remote(0) for _ in range(num_actors)]
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = OutputNode(out)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        output_channels = compiled_dag.execute(1)
        # TODO(swang): Replace with fake ObjectRef.
        results = [chan.begin_read() for chan in output_channels]
        assert results == [i + 1] * num_actors
        for chan in output_channels:
            chan.end_read()


def test_dag_errors(ray_start_regular):
    a = Actor.remote(0)
    dag = a.inc.bind(1)
    with pytest.raises(
        ValueError, match="Compiled DAGs currently require exactly one InputNode"
    ):
        dag.experimental_compile()

    a2 = Actor.remote(0)
    with InputNode() as inp:
        dag = OutputNode([a.inc.bind(inp), a2.inc.bind(1)])
    with pytest.raises(
        ValueError,
        match="Compiled DAGs require each task to take a ray.dag.InputNode or "
        "at least one other DAGNode as an input",
    ):
        dag.experimental_compile()

    @ray.remote
    def f(x):
        return x

    with InputNode() as inp:
        dag = f.bind(inp)
    with pytest.raises(
        ValueError, match="Compiled DAGs currently only support actor method nodes"
    ):
        dag.experimental_compile()

    with InputNode() as inp:
        dag = a.inc_two.bind(inp[0], inp[1])
    with pytest.raises(
        ValueError,
        match="Compiled DAGs currently do not support kwargs or multiple args for InputNode",
    ):
        dag.experimental_compile()

    with InputNode() as inp:
        dag = a.inc_two.bind(inp.x, inp.y)
    with pytest.raises(
        ValueError,
        match="Compiled DAGs currently do not support kwargs or multiple args for InputNode",
    ):
        dag.experimental_compile()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
