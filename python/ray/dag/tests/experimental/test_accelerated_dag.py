# coding: utf-8
import asyncio
import copy
import logging
import os
import pickle
import random
import sys
import time

import pytest

from ray.experimental.compiled_dag_ref import RayDAGTaskError
import ray
import ray._private
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


@ray.remote
class Collector:
    def __init__(self):
        self.results = []

    def collect(self, x):
        self.results.append(x)
        return self.results

    def collect_two(self, x, y):
        self.results.append(x)
        self.results.append(y)
        return self.results

    def collect_three(self, x, y, z):
        self.results.append(x)
        self.results.append(y)
        self.results.append(z)
        return self.results


def test_basic(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as i:
        dag = a.inc.bind(i)

    compiled_dag = dag.experimental_compile()
    dag_id = compiled_dag.get_id()

    for i in range(3):
        ref = compiled_dag.execute(1)
        assert str(ref) == f"CompiledDAGRef({dag_id}, execution_index={i})"
        result = ray.get(ref)
        assert result == i + 1

    # Note: must teardown before starting a new Ray session, otherwise you'll get
    # a segfault from the dangling monitor thread upon the new Ray init.
    compiled_dag.teardown()


def test_out_of_order_get(ray_start_regular):
    c = Collector.remote()
    with InputNode() as i:
        dag = c.collect.bind(i)

    compiled_dag = dag.experimental_compile()

    ref_a = compiled_dag.execute("a")
    ref_b = compiled_dag.execute("b")

    result_b = ray.get(ref_b)
    assert result_b == ["a", "b"]
    result_a = ray.get(ref_a)
    assert result_a == ["a"]

    compiled_dag.teardown()


def test_actor_multi_methods(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)
        dag = a.echo.bind(dag)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(1)
    result = ray.get(ref)
    assert result == 1

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
    ref = compiled_dag.execute(1)
    result = ray.get(ref)
    # test that double_and_inc() is called after inc() on actor1
    assert result == [4, 1]

    compiled_dag.teardown()


def test_actor_method_multi_binds(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)
        dag = a.inc.bind(dag)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(1)
    result = ray.get(ref)
    assert result == 2

    compiled_dag.teardown()


def test_actor_method_bind_same_constant(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc_two.bind(inp, 1)
        dag2 = a.inc_two.bind(dag, 1)
        # a.inc_two() binding the same constant "1" (i.e. non-DAGNode)
        # multiple times should not throw an exception.

    compiled_dag = dag2.experimental_compile()
    ref = compiled_dag.execute(1)
    result = ray.get(ref)
    assert result == 5

    compiled_dag.teardown()


def test_regular_args(ray_start_regular):
    # Test passing regular args to .bind in addition to DAGNode args.
    a = Actor.remote(0)
    with InputNode() as i:
        dag = a.inc_two.bind(2, i)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        ref = compiled_dag.execute(1)
        result = ray.get(ref)
        assert result == (i + 1) * 3

    compiled_dag.teardown()


def test_multi_args_basic(ray_start_regular):
    a1 = Actor.remote(0)
    a2 = Actor.remote(0)
    c = Collector.remote()
    with InputNode() as i:
        branch1 = a1.inc.bind(i[0])
        branch2 = a2.inc.bind(i[1])
        dag = c.collect_two.bind(branch2, branch1)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute(2, 3)
    result = ray.get(ref)
    assert result == [3, 2]

    compiled_dag.teardown()


def test_multi_args_single_actor(ray_start_regular):
    c = Collector.remote()
    with InputNode() as i:
        dag = c.collect_two.bind(i[1], i[0])

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        ref = compiled_dag.execute(2, 3)
        result = ray.get(ref)
        assert result == [3, 2] * (i + 1)

    compiled_dag.teardown()


def test_multi_args_branch(ray_start_regular):
    a = Actor.remote(0)
    c = Collector.remote()
    with InputNode() as i:
        branch = a.inc.bind(i[0])
        dag = c.collect_two.bind(branch, i[1])

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute(2, 3)
    result = ray.get(ref)
    assert result == [2, 3]

    compiled_dag.teardown()


def test_kwargs_basic(ray_start_regular):
    a1 = Actor.remote(0)
    a2 = Actor.remote(0)
    c = Collector.remote()
    with InputNode() as i:
        branch1 = a1.inc.bind(i.x)
        branch2 = a2.inc.bind(i.y)
        dag = c.collect_two.bind(branch2, branch1)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute(x=2, y=3)
    result = ray.get(ref)
    assert result == [3, 2]

    compiled_dag.teardown()


def test_kwargs_single_actor(ray_start_regular):
    c = Collector.remote()
    with InputNode() as i:
        dag = c.collect_two.bind(i.y, i.x)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        ref = compiled_dag.execute(x=2, y=3)
        result = ray.get(ref)
        assert result == [3, 2] * (i + 1)

    compiled_dag.teardown()


def test_kwargs_branch(ray_start_regular):
    a = Actor.remote(0)
    c = Collector.remote()
    with InputNode() as i:
        branch = a.inc.bind(i.x)
        dag = c.collect_two.bind(i.y, branch)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute(x=2, y=3)
    result = ray.get(ref)
    assert result == [3, 2]

    compiled_dag.teardown()


def test_multi_args_and_kwargs(ray_start_regular):
    a1 = Actor.remote(0)
    a2 = Actor.remote(0)
    c = Collector.remote()
    with InputNode() as i:
        branch1 = a1.inc.bind(i[0])
        branch2 = a2.inc.bind(i.y)
        dag = c.collect_three.bind(branch2, i.z, branch1)

    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute(2, y=3, z=4)
    result = ray.get(ref)
    assert result == [3, 4, 2]

    compiled_dag.teardown()


@pytest.mark.parametrize("num_actors", [1, 4])
def test_scatter_gather_dag(ray_start_regular, num_actors):
    actors = [Actor.remote(0) for _ in range(num_actors)]
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = MultiOutputNode(out)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        ref = compiled_dag.execute(1)
        results = ray.get(ref)
        assert results == [i + 1] * num_actors

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
        ref = compiled_dag.execute([])
        result = ray.get(ref)
        assert result == list(range(num_actors))

    compiled_dag.teardown()


def test_dag_exception(ray_start_regular, capsys):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute("hello")
    result = ray.get(ref)
    assert isinstance(result, RayDAGTaskError)
    assert isinstance(result.cause, TypeError)

    # Can do it multiple times.
    ref = compiled_dag.execute("hello")
    result = ray.get(ref)
    assert isinstance(result, RayDAGTaskError)
    assert isinstance(result.cause, TypeError)

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
    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(1)
    with pytest.raises(
        TypeError,
        match=(
            "wait\(\) does not support CompiledDAGRef. "
            "Please call ray.get\(\) on the CompiledDAGRef to get the result."
        ),
    ):
        ray.wait([ref])

    with pytest.raises(TypeError, match=r".*was found to be non-serializable.*"):
        ray.put([ref])

    with pytest.raises(ValueError, match="CompiledDAGRef cannot be copied."):
        copy.copy(ref)

    with pytest.raises(ValueError, match="CompiledDAGRef cannot be deep copied."):
        copy.deepcopy(ref)

    with pytest.raises(ValueError, match="CompiledDAGRef cannot be pickled."):
        pickle.dumps(ref)

    with pytest.raises(
        TypeError, match="CompiledDAGRef cannot be used as Ray task/actor argument."
    ):
        f.remote(ref)

    with pytest.raises(
        TypeError, match="CompiledDAGRef cannot be used as Ray task/actor argument."
    ):
        a2.inc.remote(ref)

    result = ray.get(ref)
    assert result == 1

    with pytest.raises(
        ValueError,
        match=(
            "ray.get\(\) can only be called once "
            "on a CompiledDAGRef and it was already called."
        ),
    ):
        ray.get(ref)
    compiled_dag.teardown()


def test_exceed_max_buffered_results(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as i:
        dag = a.inc.bind(i)

    compiled_dag = dag.experimental_compile(max_buffered_results=1)

    refs = []
    for i in range(3):
        ref = compiled_dag.execute(1)
        # Hold the refs to avoid get() being called on the ref
        # when it goes out of scope
        refs.append(ref)

    # ray.get() on the 3rd ref fails because the DAG cannot buffer 2 results.
    with pytest.raises(
        ValueError,
        match=(
            "Too many buffered results: the allowed max count for buffered "
            "results is 1; call ray.get\(\) on previous CompiledDAGRefs to "
            "free them up from buffer"
        ),
    ):
        ray.get(ref)

    del refs
    compiled_dag.teardown()


def test_compiled_dag_ref_del(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)

    compiled_dag = dag.experimental_compile()
    # Test that when ref is deleted or goes out of scope, the corresponding
    # execution result is retrieved and immediately discarded. This is confirmed
    # when future execute() methods do not block.
    for _ in range(10):
        ref = compiled_dag.execute(1)
        del ref

    compiled_dag.teardown()


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
        ref = compiled_dag.execute(i)
        results = ray.get(ref)
        assert results == i

    execution_error = None
    for i in range(99):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        if isinstance(result, RayDAGTaskError):
            execution_error = result
            break
    assert execution_error is not None
    assert isinstance(execution_error.cause, RuntimeError)

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
        ref = compiled_dag.execute(i)
        results = ray.get(ref)
        assert results == i

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
        ref = compiled_dag.execute(1)
        results = ray.get(ref)
        assert results == [i + 1] * 4

    execution_error = None
    for i in range(99):
        ref = compiled_dag.execute(1)
        res = ray.get(ref)
        if isinstance(res[0], RayDAGTaskError):
            execution_error = res[0]
            break
    assert execution_error is not None
    assert isinstance(execution_error.cause, RuntimeError)

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
        ref = compiled_dag.execute(1)
        ray.get(ref)

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
        ref = compiled_dag.execute(1)
        results = ray.get(ref)
        assert results == [i + 1] * 4

    with pytest.raises(IOError, match="Channel closed."):
        for i in range(99):
            ref = compiled_dag.execute(1)
            ray.get(ref)

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
        ref = compiled_dag.execute(1)
        ray.get(ref)

    compiled_dag.teardown()


def test_dag_teardown_while_running(ray_start_regular_shared):
    a = Actor.remote(0)

    with InputNode() as inp:
        dag = a.sleep.bind(inp)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(3)  # 3-second slow task running async
    compiled_dag.teardown()
    try:
        ray.get(ref)  # Sanity check the channel doesn't block.
    except Exception:
        pass

    # Check we can still use the actor after first DAG teardown.
    with InputNode() as inp:
        dag = a.sleep.bind(inp)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(0.1)
    result = ray.get(ref)
    assert result == 0.1

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
        awaitable_output = await compiled_dag.execute_async(i)
        result = await awaitable_output.get()
        assert result == i

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
            awaitable_output = await compiled_dag.execute_async(1)
            result = await awaitable_output.get()
            assert result == i + 1

        execution_error = None
        for i in range(99):
            awaitable_output = await compiled_dag.execute_async(1)
            result = await awaitable_output.get()
            if isinstance(result, RayDAGTaskError):
                execution_error = result
                break
        assert execution_error is not None
        assert isinstance(execution_error.cause, RuntimeError)

    loop.run_until_complete(main())
    # Note: must teardown before starting a new Ray session, otherwise you'll get
    # a segfault from the dangling monitor thread upon the new Ray init.
    compiled_dag.teardown()


class TestCompositeChannel:
    def test_composite_channel_one_actor(self, ray_start_regular_shared):
        """
        In this test, there are three 'inc' tasks on the same Ray actor, chained
        together. Therefore, the DAG will look like this:

        Driver -> a.inc -> a.inc -> a.inc -> Driver

        All communication between the driver and the actor will be done through remote
        channels, i.e., shared memory channels. All communication between the actor
        tasks will be conducted through local channels, i.e., IntraProcessChannel in
        this case.

        To elaborate, all output channels of the actor DAG nodes will be
        CompositeChannel, and the first two will have a local channel, while the last
        one will have a remote channel.
        """
        a = Actor.remote(0)
        with InputNode() as inp:
            dag = a.inc.bind(inp)
            dag = a.inc.bind(dag)
            dag = a.inc.bind(dag)

        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(1)
        assert ray.get(ref) == 4

        ref = compiled_dag.execute(2)
        assert ray.get(ref) == 24

        ref = compiled_dag.execute(3)
        assert ray.get(ref) == 108

        compiled_dag.teardown()

    def test_composite_channel_two_actors(self, ray_start_regular_shared):
        """
        In this test, there are three 'inc' tasks on the two Ray actors, chained
        together. Therefore, the DAG will look like this:

        Driver -> a.inc -> b.inc -> a.inc -> Driver

        All communication between the driver and actors will be done through remote
        channels. Also, all communication between the actor tasks will be conducted
        through remote channels, i.e., shared memory channel in this case because no
        consecutive tasks are on the same actor.
        """
        a = Actor.remote(0)
        b = Actor.remote(100)
        with InputNode() as inp:
            dag = a.inc.bind(inp)
            dag = b.inc.bind(dag)
            dag = a.inc.bind(dag)

        # a: 0+1 -> b: 100+1 -> a: 1+101
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(1)
        assert ray.get(ref) == 102

        # a: 102+2 -> b: 101+104 -> a: 104+205
        ref = compiled_dag.execute(2)
        assert ray.get(ref) == 309

        # a: 309+3 -> b: 205+312 -> a: 312+517
        ref = compiled_dag.execute(3)
        assert ray.get(ref) == 829

        compiled_dag.teardown()

    def test_composite_channel_multi_output(self, ray_start_regular_shared):
        """
        Driver -> a.inc -> a.inc ---> Driver
                        |         |
                        -> b.inc -

        All communication in this DAG will be done through CompositeChannel.
        Under the hood, the communication between two `a.inc` tasks will
        be done through a local channel, i.e., IntraProcessChannel in this
        case, while the communication between `a.inc` and `b.inc` will be
        done through a shared memory channel.
        """
        a = Actor.remote(0)
        b = Actor.remote(100)
        with InputNode() as inp:
            dag = a.inc.bind(inp)
            dag = MultiOutputNode([a.inc.bind(dag), b.inc.bind(dag)])

        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(1)
        assert ray.get(ref) == [2, 101]

        ref = compiled_dag.execute(3)
        assert ray.get(ref) == [10, 106]

        compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
