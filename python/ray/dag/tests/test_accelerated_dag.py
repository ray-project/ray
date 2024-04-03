# coding: utf-8
import logging
import os
import random
import sys
import time
import asyncio

import pytest

import ray
import ray.cluster_utils
from ray.exceptions import RaySystemError
from ray.dag import InputNode, MultiOutputNode
from ray.tests.conftest import *  # noqa
from ray._private.utils import (
    get_or_create_event_loop,
)


logger = logging.getLogger(__name__)

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)


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

    def echo(self, x):
        return x

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
        results = output_channels.begin_read()
        assert results == [i + 1] * num_actors
        output_channels.end_read()

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
        results = output_channels.begin_read()
        assert results == [i + 1] * 4
        output_channels.end_read()

    with pytest.raises(ValueError):
        for i in range(99):
            output_channels = compiled_dag.execute(1)
            output_channels.begin_read()
            output_channels.end_read()

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
        results = output_channels.begin_read()
        assert results == [i + 1] * 4
        output_channels.end_read()

    with pytest.raises(RaySystemError, match="Channel closed."):
        for i in range(99):
            output_channels = compiled_dag.execute(1)
            output_channels.begin_read()
            output_channels.end_read()


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


@pytest.mark.parametrize("max_queue_size", [None, 2])
def test_asyncio(ray_start_regular_shared, max_queue_size):
    a = Actor.remote(0)
    with InputNode() as i:
        dag = a.echo.bind(i)

    loop = get_or_create_event_loop()
    compiled_dag = dag.experimental_compile(
        enable_asyncio=True, async_max_queue_size=max_queue_size
    )

    async def main(i):
        output_channel = await compiled_dag.execute_async(i)
        # Using context manager.
        async with output_channel as result:
            assert result == i

        # Using begin_read() / end_read().
        output_channel = await compiled_dag.execute_async(i)
        result = await output_channel.begin_read()
        assert result == i
        output_channel.end_read()

    loop.run_until_complete(asyncio.gather(*[main(i) for i in range(10)]))
    # Note: must teardown before starting a new Ray session, otherwise you'll get
    # a segfault from the dangling monitor thread upon the new Ray init.
    compiled_dag.teardown()


@pytest.mark.parametrize("max_queue_size", [None, 2])
def test_asyncio_exceptions(ray_start_regular_shared, max_queue_size):
    a = Actor.remote(0, fail_after=100)
    with InputNode() as i:
        dag = a.inc.bind(i)

    loop = get_or_create_event_loop()
    compiled_dag = dag.experimental_compile(
        enable_asyncio=True, async_max_queue_size=max_queue_size
    )

    async def main():
        for i in range(99):
            output_channel = await compiled_dag.execute_async(1)
            async with output_channel as result:
                assert result == i + 1

        # Using context manager.
        exc = None
        for i in range(99):
            output_channel = await compiled_dag.execute_async(1)
            async with output_channel as result:
                if isinstance(result, Exception):
                    exc = result
        assert isinstance(exc, ValueError), exc

        # Using begin_read() / end_read().
        exc = None
        for i in range(99):
            output_channel = await compiled_dag.execute_async(1)
            try:
                result = await output_channel.begin_read()
            except Exception as e:
                exc = e
            output_channel.end_read()
        assert isinstance(exc, ValueError), exc

    loop.run_until_complete(main())
    # Note: must teardown before starting a new Ray session, otherwise you'll get
    # a segfault from the dangling monitor thread upon the new Ray init.
    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
