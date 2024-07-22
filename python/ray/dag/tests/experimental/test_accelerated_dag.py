# coding: utf-8
import asyncio
import copy
import logging
import os
import pickle
import random
import re
import sys
import time
import numpy as np

import pytest

from ray.exceptions import RayChannelError, RayChannelTimeoutError
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
import ray
import ray._private
import ray.cluster_utils
from ray.dag import InputNode, MultiOutputNode
from ray.tests.conftest import *  # noqa
from ray._private.utils import (
    get_or_create_event_loop,
)


logger = logging.getLogger(__name__)


pytestmark = [
    pytest.mark.skipif(
        sys.platform != "linux" and sys.platform != "darwin",
        reason="Requires Linux or MacOS",
    ),
    pytest.mark.timeout(500),
]


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

    @ray.method(num_returns=2)
    def return_two(self, x):
        return x, x + 1


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
        dag = a.echo.bind(i)

    compiled_dag = dag.experimental_compile()
    dag_id = compiled_dag.get_id()

    for i in range(3):
        # Use numpy so that the value returned by ray.get will be zero-copy
        # deserialized. If there is a memory leak in the DAG backend, then only
        # the first iteration will succeed.
        val = np.ones(100) * i
        ref = compiled_dag.execute(val)
        assert str(ref) == f"CompiledDAGRef({dag_id}, execution_index={i})"
        result = ray.get(ref)
        assert (result == val).all()
        # Delete the buffer so that the next DAG output can be written.
        del result

    # Note: must teardown before starting a new Ray session, otherwise you'll get
    # a segfault from the dangling monitor thread upon the new Ray init.
    compiled_dag.teardown()


def test_multiple_returns_not_supported(ray_start_regular):
    a = Actor.remote(0)
    b = Actor.remote(0)
    with InputNode() as i:
        dag = a.return_two.bind(i)
        dag = b.echo.bind(dag)

    with pytest.raises(
        ValueError,
        match="Compiled DAGs only supports actor methods with " "num_returns=1",
    ):
        dag.experimental_compile()


def test_kwargs_not_supported(ray_start_regular):
    a = Actor.remote(0)

    # Binding InputNode as kwarg is not supported.
    with InputNode() as i:
        dag = a.inc_two.bind(x=i, y=1)
    with pytest.raises(
        ValueError,
        match=r"Compiled DAG currently does not support binding to other DAG "
        "nodes as kwargs",
    ):
        compiled_dag = dag.experimental_compile()

    # Binding another DAG node as kwarg is not supported.
    with InputNode() as i:
        dag = a.inc.bind(i)
        dag = a.inc_two.bind(x=dag, y=1)
    with pytest.raises(
        ValueError,
        match=r"Compiled DAG currently does not support binding to other DAG "
        "nodes as kwargs",
    ):
        compiled_dag = dag.experimental_compile()

    # Binding normal Python value as a kwarg is supported.
    with InputNode() as i:
        dag = a.inc_two.bind(i, y=1)
    compiled_dag = dag.experimental_compile()
    assert ray.get(compiled_dag.execute(2)) == 3

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

    with pytest.raises(
        ValueError,
        match=r"dag.execute\(\) or dag.execute_async\(\) must be called with 2 "
        "positional args, got 1",
    ):
        compiled_dag.execute((2, 3))

    with pytest.raises(
        ValueError,
        match=r"dag.execute\(\) or dag.execute_async\(\) must be called with 2 "
        "positional args, got 0",
    ):
        compiled_dag.execute()

    with pytest.raises(
        ValueError,
        match=r"dag.execute\(\) or dag.execute_async\(\) must be called with 2 "
        "positional args, got 0",
    ):
        compiled_dag.execute(args=(2, 3))

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

    with pytest.raises(
        ValueError,
        match=r"dag.execute\(\) or dag.execute_async\(\) must be called with kwarg",
    ):
        compiled_dag.execute()

    with pytest.raises(
        ValueError,
        match=r"dag.execute\(\) or dag.execute_async\(\) must be called with kwarg `x`",
    ):
        compiled_dag.execute(y=3)

    with pytest.raises(
        ValueError,
        match=r"dag.execute\(\) or dag.execute_async\(\) must be called with kwarg `y`",
    ):
        compiled_dag.execute(x=3)

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


def test_execution_timeout(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)

    compiled_dag = dag.experimental_compile(_execution_timeout=2)
    refs = []
    timed_out = False
    epsilon = 0.1  # Allow for some slack in the timeout checking
    for i in range(5):
        try:
            start_time = time.monotonic()
            ref = compiled_dag.execute(1)
            # Hold the refs to avoid get() being called on the ref
            # in `__del__()` when it goes out of scope
            refs.append(ref)
        except RayChannelTimeoutError:
            duration = time.monotonic() - start_time
            assert duration > 2 - epsilon
            assert duration < 2 + epsilon
            # The first 3 tasks should complete, and the 4th one
            # should block then time out because the max possible
            # concurrent executions for the DAG is 3. See the
            # following diagram:
            # driver -(3)-> a.inc (2) -(1)-> driver
            assert i == 3
            timed_out = True
            break
    assert timed_out

    compiled_dag.teardown()


def test_get_timeout(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.sleep.bind(inp)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(10)

    timed_out = False
    epsilon = 0.1  # Allow for some slack in the timeout checking
    try:
        start_time = time.monotonic()
        ray.get(ref, timeout=3)
    except RayChannelTimeoutError:
        duration = time.monotonic() - start_time
        assert duration > 3 - epsilon
        assert duration < 3 + epsilon
        timed_out = True
    assert timed_out

    compiled_dag.teardown()


def test_buffered_get_timeout(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.sleep.bind(inp)

    compiled_dag = dag.experimental_compile()
    refs = []
    for i in range(3):
        # sleeps 1, 2, 3 seconds, respectively
        ref = compiled_dag.execute(i + 1)
        refs.append(ref)

    with pytest.raises(RayChannelTimeoutError):
        # Since the first two sleep() tasks need to complete before
        # the last one, the total time needed is 1 + 2 + 3 = 6 seconds,
        # therefore with a timeout of 3.5 seconds, an exception will
        # be raised.
        ray.get(refs[-1], timeout=3.5)

    compiled_dag.teardown()


def test_get_with_zero_timeout(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(1)
    # Give enough time for DAG execution result to be ready
    time.sleep(1)
    # Use timeout=0 to either get result immediately or raise an exception
    result = ray.get(ref, timeout=0)
    assert result == 1

    compiled_dag.teardown()


def test_dag_exception_basic(ray_start_regular, capsys):
    # Test application throwing exceptions with a single task.
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)

    # Can throw an error.
    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute("hello")
    with pytest.raises(TypeError) as exc_info:
        ray.get(ref)
    # Traceback should match the original actor class definition.
    assert "self.i += x" in str(exc_info.value)

    # Can throw an error multiple times.
    ref = compiled_dag.execute("hello")
    with pytest.raises(TypeError) as exc_info:
        ray.get(ref)
    # Traceback should match the original actor class definition.
    assert "self.i += x" in str(exc_info.value)

    # Can use the DAG after exceptions are thrown.
    assert ray.get(compiled_dag.execute(1)) == 1

    compiled_dag.teardown()


def test_dag_exception_chained(ray_start_regular, capsys):
    # Test application throwing exceptions with a task that depends on another
    # task.
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)
        dag = a.inc.bind(dag)

    # Can throw an error.
    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute("hello")
    with pytest.raises(TypeError) as exc_info:
        ray.get(ref)
    # Traceback should match the original actor class definition.
    assert "self.i += x" in str(exc_info.value)

    # Can throw an error multiple times.
    ref = compiled_dag.execute("hello")
    with pytest.raises(TypeError) as exc_info:
        ray.get(ref)
    # Traceback should match the original actor class definition.
    assert "self.i += x" in str(exc_info.value)

    # Can use the DAG after exceptions are thrown.
    assert ray.get(compiled_dag.execute(1)) == 2

    compiled_dag.teardown()


def test_dag_exception_multi_output(ray_start_regular, capsys):
    # Test application throwing exceptions with a DAG with multiple outputs.
    a = Actor.remote(0)
    b = Actor.remote(0)
    with InputNode() as inp:
        dag = MultiOutputNode([a.inc.bind(inp), b.inc.bind(inp)])

    compiled_dag = dag.experimental_compile()

    # Can throw an error.
    ref = compiled_dag.execute("hello")
    with pytest.raises(TypeError) as exc_info:
        ray.get(ref)
    # Traceback should match the original actor class definition.
    assert "self.i += x" in str(exc_info.value)

    # Can throw an error multiple times.
    ref = compiled_dag.execute("hello")
    with pytest.raises(TypeError) as exc_info:
        ray.get(ref)
    # Traceback should match the original actor class definition.
    assert "self.i += x" in str(exc_info.value)

    # Can use the DAG after exceptions are thrown.
    assert ray.get(compiled_dag.execute(1)) == [1, 1]

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
            re.escape(
                "wait() expected a list of ray.ObjectRef or ray.ObjectRefGenerator, "
                "got <class 'ray.experimental.compiled_dag_ref.CompiledDAGRef'>"
            )
        ),
    ):
        ray.wait(ref)

    with pytest.raises(
        TypeError,
        match=(
            re.escape(
                "wait() expected a list of ray.ObjectRef or ray.ObjectRefGenerator, "
                "got list containing "
                "<class 'ray.experimental.compiled_dag_ref.CompiledDAGRef'>"
            )
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
            "on a CompiledDAGRef, and it was already called."
        ),
    ):
        ray.get(ref)
    compiled_dag.teardown()


def test_exceed_max_buffered_results(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as i:
        dag = a.inc.bind(i)

    compiled_dag = dag.experimental_compile(_max_buffered_results=1)

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

    with pytest.raises(RuntimeError):
        for i in range(99):
            ref = compiled_dag.execute(i)
            results = ray.get(ref)
            assert results == i

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

    with pytest.raises(RuntimeError):
        for i in range(99, 200):
            ref = compiled_dag.execute(1)
            results = ray.get(ref)
            assert results == [i + 1] * 4

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

    with pytest.raises(RayChannelError, match="Channel closed."):
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
        enable_asyncio=True, _asyncio_max_queue_size=max_queue_size
    )

    async def main(i):
        # Use numpy so that the return value will be zero-copy deserialized. If
        # there is a memory leak in the DAG backend, then only the first task
        # will succeed.
        val = np.ones(100) * i
        fut = await compiled_dag.execute_async(val)
        result = await fut
        assert (result == val).all()

    loop.run_until_complete(asyncio.gather(*[main(i) for i in range(10)]))
    # Note: must teardown before starting a new Ray session, otherwise you'll get
    # a segfault from the dangling monitor thread upon the new Ray init.
    compiled_dag.teardown()


@pytest.mark.parametrize("max_queue_size", [None, 2])
def test_asyncio_exceptions(ray_start_regular_shared, max_queue_size):
    a = Actor.remote(0)
    with InputNode() as i:
        dag = a.inc.bind(i)

    loop = get_or_create_event_loop()
    compiled_dag = dag.experimental_compile(
        enable_asyncio=True, _asyncio_max_queue_size=max_queue_size
    )

    async def main():
        fut = await compiled_dag.execute_async(1)
        result = await fut
        assert result == 1

        fut = await compiled_dag.execute_async("hello")
        with pytest.raises(TypeError) as exc_info:
            await fut
        # Traceback should match the original actor class definition.
        assert "self.i += x" in str(exc_info.value)

        # Can throw an error multiple times.
        fut = await compiled_dag.execute_async("hello")
        with pytest.raises(TypeError) as exc_info:
            await fut
        # Traceback should match the original actor class definition.
        assert "self.i += x" in str(exc_info.value)

        # Can use the DAG after exceptions are thrown.
        fut = await compiled_dag.execute_async(1)
        result = await fut
        assert result == 2

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

    def test_intra_process_channel_with_multi_readers(self, ray_start_regular_shared):
        """
        In this test, there are three 'echo' tasks on the same Ray actor.
        The DAG will look like this:

        Driver -> a.echo -> a.echo -> Driver
                         |         |
                         -> a.echo -

        All communication between the driver and the actor will be done through remote
        channels, i.e., shared memory channels. All communication between the actor
        tasks will be conducted through local channels, i.e., IntraProcessChannel in
        this case.
        """
        a = Actor.remote(0)
        with InputNode() as inp:
            dag = a.echo.bind(inp)
            x = a.echo.bind(dag)
            y = a.echo.bind(dag)
            dag = MultiOutputNode([x, y])

        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(1)
        assert ray.get(ref) == [1, 1]

        ref = compiled_dag.execute(2)
        assert ray.get(ref) == [2, 2]

        ref = compiled_dag.execute(3)
        assert ray.get(ref) == [3, 3]

        compiled_dag.teardown()


def test_simulate_pipeline_parallelism(ray_start_regular_shared):
    """
    This pattern simulates the case of pipeline parallelism training, where `w0_input`
    reads data from the driver, and the fan-out tasks, `d00`, `d01`, and `d02`, use
    `IntraProcessChannel` to read the data as the input for the forward pass.

    Compared to reading data from shared memory channels for each forward pass, using
    `IntraProcessChannel` may be more efficient because it avoids the overhead of
    deserialization for each forward pass.
    """

    @ray.remote
    class Worker:
        def __init__(self, rank):
            self.rank = rank
            self.logs = []

        def forward(self, data, idx):
            batch_id = data[idx]
            self.logs.append(f"FWD rank-{self.rank}, batch-{batch_id}")
            return batch_id

        def backward(self, batch_id):
            self.logs.append(f"BWD rank-{self.rank}, batch-{batch_id}")
            return batch_id

        def get_logs(self):
            return self.logs

        def read_input(self, input):
            return input

    worker_0 = Worker.remote(0)
    worker_1 = Worker.remote(1)

    # Worker 0: FFFBBB
    # Worker 1: BBB
    with InputNode() as inp:
        w0_input = worker_0.read_input.bind(inp)
        d00 = worker_0.forward.bind(w0_input, 0)  # worker_0 FWD
        d01 = worker_0.forward.bind(w0_input, 1)  # worker_0 FWD
        d02 = worker_0.forward.bind(w0_input, 2)  # worker_0 FWD

        d10 = worker_1.backward.bind(d00)  # worker_1 BWD
        d11 = worker_1.backward.bind(d01)  # worker_1 BWD
        d12 = worker_1.backward.bind(d02)  # worker_1 BWD

        d03 = worker_0.backward.bind(d10)  # worker_0 BWD
        d04 = worker_0.backward.bind(d11)  # worker_0 BWD
        d05 = worker_0.backward.bind(d12)  # worker_0 BWD

        output_dag = MultiOutputNode([d03, d04, d05])

    output_dag = output_dag.experimental_compile()
    res = output_dag.execute([0, 1, 2])

    assert ray.get(res) == [0, 1, 2]
    # Worker 0: FFFBBB
    assert ray.get(worker_0.get_logs.remote()) == [
        "FWD rank-0, batch-0",
        "FWD rank-0, batch-1",
        "FWD rank-0, batch-2",
        "BWD rank-0, batch-0",
        "BWD rank-0, batch-1",
        "BWD rank-0, batch-2",
    ]
    # Worker 1: BBB
    assert ray.get(worker_1.get_logs.remote()) == [
        "BWD rank-1, batch-0",
        "BWD rank-1, batch-1",
        "BWD rank-1, batch-2",
    ]
    output_dag.teardown()


def test_channel_read_after_close(ray_start_regular_shared):
    # Tests that read to a channel after accelerated DAG teardown raises a
    # RayChannelError exception as the channel is closed (see issue #46284).
    @ray.remote
    class Actor:
        def foo(self, arg):
            return arg

    a = Actor.remote()
    with InputNode() as inp:
        dag = a.foo.bind(inp)

    dag = dag.experimental_compile()
    ref = dag.execute(1)
    dag.teardown()

    with pytest.raises(RayChannelError, match="Channel closed."):
        ray.get(ref)


def test_channel_write_after_close(ray_start_regular_shared):
    # Tests that write to a channel after accelerated DAG teardown raises a
    # RayChannelError exception as the channel is closed.
    @ray.remote
    class Actor:
        def foo(self, arg):
            return arg

    a = Actor.remote()
    with InputNode() as inp:
        dag = a.foo.bind(inp)

    dag = dag.experimental_compile()
    dag.teardown()

    with pytest.raises(RayChannelError, match="Channel closed."):
        dag.execute(1)


def test_driver_and_actor_as_readers(ray_start_cluster):
    a = Actor.remote(0)
    b = Actor.remote(10)
    with InputNode() as inp:
        x = a.inc.bind(inp)
        y = b.inc.bind(x)
        dag = MultiOutputNode([x, y])

    with pytest.raises(
        ValueError,
        match="DAG outputs currently can only be read by the driver--not the driver "
        "and actors.",
    ):
        dag.experimental_compile()


def test_payload_large(ray_start_cluster):
    cluster = ray_start_cluster
    # This node is for the driver (including the CompiledDAG.DAGDriverProxyActor).
    first_node_handle = cluster.add_node(num_cpus=1)
    # This node is for the reader.
    second_node_handle = cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    nodes = [first_node_handle.node_id, second_node_handle.node_id]
    # We want to check that there are two nodes. Thus, we convert `nodes` to a set and
    # then back to a list to remove duplicates. Then we check that the length of `nodes`
    # is 2.
    nodes = list(set(nodes))
    assert len(nodes) == 2

    def create_actor(node):
        return Actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(node, soft=False)
        ).remote(0)

    def get_node_id(self):
        return ray.get_runtime_context().get_node_id()

    driver_node = get_node_id(None)
    nodes.remove(driver_node)

    a = create_actor(nodes[0])
    a_node = ray.get(a.__ray_call__.remote(get_node_id))
    assert a_node == nodes[0]
    # Check that the driver and actor are on different nodes.
    assert driver_node != a_node

    with InputNode() as i:
        dag = a.echo.bind(i)

    compiled_dag = dag.experimental_compile()

    # Ray sets the gRPC payload max size to 512 MiB. We choose a size in this test that
    # is a bit larger.
    size = 1024 * 1024 * 600
    val = b"x" * size

    for i in range(3):
        ref = compiled_dag.execute(val)
        result = ray.get(ref)
        assert result == val

    # Note: must teardown before starting a new Ray session, otherwise you'll get
    # a segfault from the dangling monitor thread upon the new Ray init.
    compiled_dag.teardown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
