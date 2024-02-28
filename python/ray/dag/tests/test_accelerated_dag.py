# coding: utf-8
import logging
import os
import random
import sys
import time

import pytest

import ray
import ray.cluster_utils
from ray.exceptions import RaySystemError
from ray.dag import InputNode, MultiOutputNode
from ray.tests.conftest import *  # noqa


logger = logging.getLogger(__name__)

if sys.platform != "linux":
    pytest.skip("Skipping, requires Linux.", allow_module_level=True)


@ray.remote
class Actor:
    def __init__(self, init_value, fail_after=None, sys_exit=False):
        print("__init__ PID", os.getpid())
        self.i = init_value
        self.fail_after = fail_after
        self.sys_exit = sys_exit

    def inc(self, x):
        self.i += x
        if self.fail_after and self.i > self.fail_after:
            # Randomize the failures to better cover multi actor scenarios.
            if random.random() > 0.5:
                if self.sys_exit:
                    os._exit(1)
                else:
                    raise ValueError("injected fault")
        return self.i

    def append_to(self, lst):
        lst.append(self.i)
        return lst

    def inc_two(self, x, y):
        self.i += x
        self.i += y
        return self.i

    def sleep(self, x):
        time.sleep(x)
        return x


def test_basic(ray_start_regular):
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

    # Note: must teardown before starting a new Ray session, otherwise you'll get
    # a segfault from the dangling monitor thread upon the new Ray init.
    compiled_dag.teardown()


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

    compiled_dag.teardown()


@pytest.mark.parametrize("num_actors", [1, 4])
def test_scatter_gather_dag(ray_start_regular, num_actors):
    actors = [Actor.remote(0) for _ in range(num_actors)]
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = MultiOutputNode(out)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        output_channels = compiled_dag.execute(1)
        # TODO(swang): Replace with fake ObjectRef.
        results = [chan.begin_read() for chan in output_channels]
        assert results == [i + 1] * num_actors
        for chan in output_channels:
            chan.end_read()

    compiled_dag.teardown()


@pytest.mark.parametrize("num_actors", [1, 4])
def test_chain_dag(ray_start_regular, num_actors):
    actors = [Actor.remote(i) for i in range(num_actors)]
    with InputNode() as inp:
        dag = inp
        for a in actors:
            dag = a.append_to.bind(dag)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        output_channel = compiled_dag.execute([])
        # TODO(swang): Replace with fake ObjectRef.
        result = output_channel.begin_read()
        assert result == list(range(num_actors))
        output_channel.end_read()

    compiled_dag.teardown()


def test_dag_exception(ray_start_regular, capsys):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)

    compiled_dag = dag.experimental_compile()
    output_channel = compiled_dag.execute("hello")
    with pytest.raises(TypeError):
        output_channel.begin_read()
    output_channel.end_read()

    # Can do it multiple times.
    output_channel = compiled_dag.execute("hello")
    with pytest.raises(TypeError):
        output_channel.begin_read()

    compiled_dag.teardown()


def test_dag_errors(ray_start_regular):
    a = Actor.remote(0)
    dag = a.inc.bind(1)
    with pytest.raises(
        NotImplementedError,
        match="Compiled DAGs currently require exactly one InputNode",
    ):
        dag.experimental_compile()

    a2 = Actor.remote(0)
    with InputNode() as inp:
        dag = MultiOutputNode([a.inc.bind(inp), a2.inc.bind(1)])
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
        NotImplementedError,
        match="Compiled DAGs currently only support actor method nodes",
    ):
        dag.experimental_compile()

    with InputNode() as inp:
        dag = a.inc.bind(inp)
        dag = a.inc.bind(dag)
    with pytest.raises(
        NotImplementedError,
        match="Compiled DAGs can contain at most one task per actor handle.",
    ):
        dag.experimental_compile()

    with InputNode() as inp:
        dag = a.inc_two.bind(inp[0], inp[1])
    with pytest.raises(
        NotImplementedError,
        match="Compiled DAGs currently do not support kwargs or multiple args "
        "for InputNode",
    ):
        dag.experimental_compile()

    with InputNode() as inp:
        dag = a.inc_two.bind(inp.x, inp.y)
    with pytest.raises(
        NotImplementedError,
        match="Compiled DAGs currently do not support kwargs or multiple args "
        "for InputNode",
    ):
        dag.experimental_compile()


def test_dag_fault_tolerance(ray_start_regular_shared):
    actors = [Actor.remote(0, fail_after=100, sys_exit=False) for _ in range(4)]
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = MultiOutputNode(out)

    compiled_dag = dag.experimental_compile()

    for i in range(99):
        output_channels = compiled_dag.execute(1)
        # TODO(swang): Replace with fake ObjectRef.
        results = [chan.begin_read() for chan in output_channels]
        assert results == [i + 1] * 4
        for chan in output_channels:
            chan.end_read()

    with pytest.raises(ValueError):
        for i in range(99):
            output_channels = compiled_dag.execute(1)
            for chan in output_channels:
                chan.begin_read()
            for chan in output_channels:
                chan.end_read()

    compiled_dag.teardown()


def test_dag_fault_tolerance_sys_exit(ray_start_regular_shared):
    actors = [Actor.remote(0, fail_after=100, sys_exit=True) for _ in range(4)]
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = MultiOutputNode(out)

    compiled_dag = dag.experimental_compile()

    for i in range(99):
        output_channels = compiled_dag.execute(1)
        # TODO(swang): Replace with fake ObjectRef.
        results = [chan.begin_read() for chan in output_channels]
        assert results == [i + 1] * 4
        for chan in output_channels:
            chan.end_read()

    with pytest.raises(RaySystemError, match="channel closed"):
        for i in range(99):
            output_channels = compiled_dag.execute(1)
            for chan in output_channels:
                chan.begin_read()
            for chan in output_channels:
                chan.end_read()


def test_dag_teardown_while_running(ray_start_regular_shared):
    a = Actor.remote(0)

    with InputNode() as inp:
        dag = a.sleep.bind(inp)

    compiled_dag = dag.experimental_compile()
    chan = compiled_dag.execute(3)  # 3-second slow task running async
    compiled_dag.teardown()
    try:
        chan.begin_read()  # Sanity check the channel doesn't block.
    except Exception:
        pass

    # Check we can still use the actor after first DAG teardown.
    with InputNode() as inp:
        dag = a.sleep.bind(inp)

    compiled_dag = dag.experimental_compile()
    chan = compiled_dag.execute(0.1)
    result = chan.begin_read()
    assert result == 0.1
    chan.end_read()

    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
