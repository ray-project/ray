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
        self.i = init_value
        self.fail_after = fail_after
        self.sys_exit = sys_exit

        self.count = 0

    def _fail_if_needed(self):
        if self.fail_after and self.count > self.fail_after:
            # Randomize the failures to better cover multi actor scenarios.
            if random.random() > 0.5:
                if self.sys_exit:
                    os._exit(1)
                else:
                    raise RuntimeError("injected fault")

    def inc(self, x):
        self.i += x
        self.count += 1
        self._fail_if_needed()
        return self.i

    def double_and_inc(self, x):
        self.i *= 2
        self.i += x
        return self.i

    def echo(self, x):
        self.count += 1
        self._fail_if_needed()
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


def test_actor_multi_methods(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)
        dag = a.echo.bind(dag)

    compiled_dag = dag.experimental_compile()
    output_channel = compiled_dag.execute(1)
    result = output_channel.begin_read()
    assert result == 1
    output_channel.end_read()

    compiled_dag.teardown()


def test_actor_methods_execution_order(ray_start_regular):
    actor1 = Actor.remote(0)
    actor2 = Actor.remote(0)
    with InputNode() as inp:
        branch1 = actor1.inc.bind(inp)
        branch1 = actor2.double_and_inc.bind(branch1)
        branch2 = actor2.inc.bind(inp)
        branch2 = actor1.double_and_inc.bind(branch2)
        dag = MultiOutputNode([branch2, branch1])

    compiled_dag = dag.experimental_compile()
    output_channel = compiled_dag.execute(1)
    result = output_channel.begin_read()
    # test that double_and_inc() is called after inc() on actor1
    assert result == [4, 1]
    output_channel.end_read()

    compiled_dag.teardown()


def test_actor_method_multi_binds(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)
        dag = a.inc.bind(dag)

    compiled_dag = dag.experimental_compile()
    output_channel = compiled_dag.execute(1)
    result = output_channel.begin_read()
    assert result == 2
    output_channel.end_read()

    compiled_dag.teardown()


def test_actor_method_bind_same_constant(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc_two.bind(inp, 1)
        dag2 = a.inc_two.bind(dag, 1)
        # a.inc_two() binding the same constant "1" (i.e. non-DAGNode)
        # multiple times should not throw an exception.

    compiled_dag = dag2.experimental_compile()
    output_channel = compiled_dag.execute(1)
    result = output_channel.begin_read()
    assert result == 5
    output_channel.end_read()

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

    with InputNode() as inp:
        dag = a.inc.bind(inp)
        dag2 = a.inc.bind(inp)
        dag3 = a.inc_two.bind(dag, dag2)
    with pytest.raises(
        NotImplementedError,
        match=r"Compiled DAGs currently do not support binding the same input "
        "on the same actor multiple times.*",
    ):
        dag3.experimental_compile()

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


def test_dag_fault_tolerance_chain(ray_start_regular_shared):
    actors = [
        Actor.remote(0, fail_after=100 if i == 0 else None, sys_exit=False)
        for i in range(4)
    ]
    with InputNode() as i:
        dag = i
        for a in actors:
            dag = a.echo.bind(dag)

    compiled_dag = dag.experimental_compile()

    for i in range(99):
        output_channels = compiled_dag.execute(i)
        # TODO(swang): Replace with fake ObjectRef.
        results = output_channels.begin_read()
        assert results == i
        output_channels.end_read()

    with pytest.raises(RuntimeError):
        for i in range(99):
            output_channels = compiled_dag.execute(i)
            output_channels.begin_read()
            output_channels.end_read()

    compiled_dag.teardown()

    # All actors are still alive.
    ray.get([actor.sleep.remote(0) for actor in actors])

    # Remaining actors can be reused.
    actors.pop(0)
    with InputNode() as i:
        dag = i
        for a in actors:
            dag = a.echo.bind(dag)

    compiled_dag = dag.experimental_compile()
    for i in range(100):
        output_channels = compiled_dag.execute(i)
        # TODO(swang): Replace with fake ObjectRef.
        results = output_channels.begin_read()
        assert results == i
        output_channels.end_read()

    compiled_dag.teardown()


def test_dag_fault_tolerance(ray_start_regular_shared):
    actors = [
        Actor.remote(0, fail_after=100 if i == 0 else None, sys_exit=False)
        for i in range(4)
    ]
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

    with pytest.raises(RuntimeError):
        for i in range(99):
            output_channels = compiled_dag.execute(1)
            output_channels.begin_read()
            output_channels.end_read()

    compiled_dag.teardown()

    # All actors are still alive.
    ray.get([actor.sleep.remote(0) for actor in actors])

    # Remaining actors can be reused.
    actors.pop(0)
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = MultiOutputNode(out)

    compiled_dag = dag.experimental_compile()
    for i in range(100):
        output_channels = compiled_dag.execute(1)
        # TODO(swang): Replace with fake ObjectRef.
        output_channels.begin_read()
        output_channels.end_read()

    compiled_dag.teardown()


def test_dag_fault_tolerance_sys_exit(ray_start_regular_shared):
    actors = [
        Actor.remote(0, fail_after=100 if i == 0 else None, sys_exit=True)
        for i in range(4)
    ]
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

    with pytest.raises(IOError, match="Channel closed."):
        for i in range(99):
            output_channels = compiled_dag.execute(1)
            output_channels.begin_read()
            output_channels.end_read()

    # Remaining actors are still alive.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actors[0].echo.remote("hello"))
    actors.pop(0)
    ray.get([actor.echo.remote("hello") for actor in actors])

    # Remaining actors can be reused.
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = MultiOutputNode(out)

    compiled_dag = dag.experimental_compile()
    for i in range(100):
        output_channels = compiled_dag.execute(1)
        # TODO(swang): Replace with fake ObjectRef.
        output_channels.begin_read()
        output_channels.end_read()

    compiled_dag.teardown()


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
        assert isinstance(exc, RuntimeError), exc

        # Using begin_read() / end_read().
        exc = None
        for i in range(99):
            output_channel = await compiled_dag.execute_async(1)
            try:
                result = await output_channel.begin_read()
            except Exception as e:
                exc = e
            output_channel.end_read()
        assert isinstance(exc, RuntimeError), exc

    loop.run_until_complete(main())
    # Note: must teardown before starting a new Ray session, otherwise you'll get
    # a segfault from the dangling monitor thread upon the new Ray init.
    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
