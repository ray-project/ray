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
import torch

import pytest


from ray._private.test_utils import run_string_as_driver
from ray.exceptions import ActorDiedError, RayChannelError, RayChannelTimeoutError
import ray
import ray._private
import ray.cluster_utils
from ray.dag import InputNode, MultiOutputNode
from ray.tests.conftest import *  # noqa
from ray._private.utils import (
    get_or_create_event_loop,
)
from ray.dag import DAGContext
from ray._private.test_utils import (
    run_string_as_driver_nonblocking,
    wait_for_pid_to_exit,
)
import signal
import psutil


logger = logging.getLogger(__name__)


pytestmark = [
    pytest.mark.skipif(
        sys.platform != "linux" and sys.platform != "darwin",
        reason="Requires Linux or MacOS",
    ),
    pytest.mark.timeout(500),
]


@pytest.fixture
def temporary_change_timeout(request):
    ctx = DAGContext.get_current()
    original = ctx.submit_timeout
    ctx.submit_timeout = request.param
    yield ctx.submit_timeout
    ctx.submit_timeout = original


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

    def read_input(self, x):
        return x

    @ray.method(num_returns=2)
    def inc_and_return_two(self, x):
        self.i += x
        return self.i, self.i + 1

    @ray.method(num_returns=1)
    def return_two_as_one(self, x):
        return x, x + 1

    @ray.method(num_returns=2)
    def return_two_from_three(self, x):
        return x, x + 1, x + 2

    @ray.method(num_returns=2)
    def return_two_but_raise_exception(self, x):
        raise RuntimeError
        return 1, 2

    def get_events(self):
        return getattr(self, "__ray_cgraph_events", [])


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
        assert (
            str(ref)
            == f"CompiledDAGRef({dag_id}, execution_index={i}, channel_index={None})"
        )
        result = ray.get(ref)
        assert (result == val).all()
        # Delete the buffer so that the next DAG output can be written.
        del result


class TestDAGRefDestruction:
    def test_basic_destruction(self, ray_start_regular):
        a = Actor.remote(0)
        with InputNode() as i:
            dag = a.echo.bind(i)
        compiled_dag = dag.experimental_compile()

        try:
            for i in range(3):
                val = np.ones(100) * i
                ref = compiled_dag.execute(val)
                # Since ref.get() is not called, the destructor releases its native
                # buffer without deserializing the value. If the destructor fails to
                # release the buffer, the subsequent DAG execution will fail due to
                # memory leak.
                del ref
        except RayChannelTimeoutError:
            pytest.fail(
                "The native buffer associated with the CompiledDAGRef was not "
                "released upon destruction."
            )

        # Ensure that subsequent DAG executions do not fail due to memory leak
        # and the results can be retrieved by ray.get().
        val = np.ones(100)
        ref = compiled_dag.execute(val)
        result = ray.get(ref)
        assert (result == val).all()
        del ref
        assert compiled_dag._result_buffer == {}
        assert compiled_dag._destructed_ref_idxs == {}
        assert compiled_dag._got_ref_idxs == {}

    def test_get_ref_before_destructed_ref(self, ray_start_regular):
        a = Actor.remote(0)
        with InputNode() as inp:
            dag = a.inc.bind(inp)
        compiled_dag = dag.experimental_compile(_max_inflight_executions=3)
        ref = compiled_dag.execute(1)
        ref2 = compiled_dag.execute(1)
        del ref2
        # Test that ray.get() on ref still works properly even if
        # ref2 (corresponding to a later execution) is destructed first
        assert ray.get(ref) == 1
        assert compiled_dag._result_buffer == {}
        assert compiled_dag._destructed_ref_idxs == {}
        assert compiled_dag._got_ref_idxs == {}

    def test_get_ref_after_destructed_ref(self, ray_start_regular):
        a = Actor.remote(0)
        with InputNode() as inp:
            dag = a.inc.bind(inp)
        compiled_dag = dag.experimental_compile()
        ref = compiled_dag.execute(2)
        ref2 = compiled_dag.execute(2)
        ref3 = compiled_dag.execute(2)
        del ref
        del ref2
        # Test that ray.get() works correctly if preceding ref was destructed
        assert ray.get(ref3) == 6
        assert compiled_dag._result_buffer == {}
        assert compiled_dag._destructed_ref_idxs == {}
        assert compiled_dag._got_ref_idxs == {}

    def test_release_buffer_on_execute(self, ray_start_regular):
        a = Actor.remote(0)
        with InputNode() as inp:
            dag = a.inc.bind(inp)
        compiled_dag = dag.experimental_compile(_max_inflight_executions=3)
        ref = compiled_dag.execute(3)
        ref2 = compiled_dag.execute(3)
        ref3 = compiled_dag.execute(3)
        del ref2
        del ref3
        ray.get(ref)
        ref4 = compiled_dag.execute(3)
        del ref4
        # Test that max_inflight error is not raised as ref2 and ref3
        # should be destructed and not counted in the inflight executions
        ref5 = compiled_dag.execute(3)
        assert ray.get(ref5) == 15
        assert compiled_dag._result_buffer == {}
        assert compiled_dag._destructed_ref_idxs == {}
        assert compiled_dag._got_ref_idxs == {}

    def test_destruct_and_get_multioutput_ref(self, ray_start_regular):
        a = Actor.remote(0)
        with InputNode() as inp:
            dag = MultiOutputNode([a.inc.bind(inp), a.inc.bind(inp)])
        compiled_dag = dag.experimental_compile()
        ref1, ref2 = compiled_dag.execute(1)
        del ref2
        # Test that ray.get() on ref1 still works properly even if
        # ref2 was destructed
        assert ray.get(ref1) == 1
        assert compiled_dag._result_buffer == {}
        assert compiled_dag._destructed_ref_idxs == {}
        assert compiled_dag._got_ref_idxs == {}

    def test_destruct_and_get_multioutput_no_leak(self, ray_start_regular):
        a = Actor.remote(0)
        with InputNode() as inp:
            dag = MultiOutputNode([a.inc.bind(inp), a.inc.bind(inp)])
        compiled_dag = dag.experimental_compile()
        ref_list = compiled_dag.execute(1)
        assert compiled_dag._result_buffer == {}
        assert compiled_dag._destructed_ref_idxs == {}
        assert compiled_dag._got_ref_idxs == {}
        ref1, ref2 = compiled_dag.execute(2)
        del ref1
        assert compiled_dag._result_buffer == {}
        assert compiled_dag._destructed_ref_idxs == {1: {0}}
        assert compiled_dag._got_ref_idxs == {}
        ray.get(ref2)
        assert compiled_dag._result_buffer == {0: {0: 1, 1: 2}}
        ray.get(ref_list)
        # Test that that ref1 doesn't stay in result_buffer
        assert compiled_dag._result_buffer == {}
        assert compiled_dag._destructed_ref_idxs == {}
        assert compiled_dag._got_ref_idxs == {}

    def test_asyncio_destruction(self, ray_start_regular):
        a = Actor.remote(0)
        b = Actor.remote(0)
        with InputNode() as i:
            dag = MultiOutputNode([a.echo.bind(i), b.echo.bind(i)])

        loop = get_or_create_event_loop()
        compiled_dag = dag.experimental_compile(enable_asyncio=True)

        async def main(i):
            # use asyncio.sleep to give back control so GC has
            # a chance to run
            await asyncio.sleep(i * 0.1)
            futs = await compiled_dag.execute_async(i)
            assert len(futs) == 2
            result = await futs[0]
            assert result == i

        loop.run_until_complete(asyncio.gather(*[main(i) for i in range(5)]))
        assert compiled_dag._result_buffer == {}
        assert compiled_dag._destructed_ref_idxs == {}
        assert compiled_dag._got_ref_idxs == {}


@pytest.mark.parametrize("single_fetch", [True, False])
def test_two_returns_one_reader(ray_start_regular, single_fetch):
    a = Actor.remote(0)
    b = Actor.remote(0)
    with InputNode() as i:
        o1, o2 = a.return_two.bind(i)
        o3 = b.echo.bind(o1)
        o4 = b.echo.bind(o2)
        dag = MultiOutputNode([o3, o4])

    compiled_dag = dag.experimental_compile()
    for _ in range(3):
        refs = compiled_dag.execute(1)
        if single_fetch:
            for i, ref in enumerate(refs):
                res = ray.get(ref)
                assert res == i + 1
        else:
            res = ray.get(refs)
            assert res == [1, 2]


@pytest.mark.parametrize("single_fetch", [True, False])
def test_two_returns_two_readers(ray_start_regular, single_fetch):
    a = Actor.remote(0)
    b = Actor.remote(0)
    c = Actor.remote(0)
    with InputNode() as i:
        o1, o2 = a.return_two.bind(i)
        o3 = b.echo.bind(o1)
        o4 = c.echo.bind(o2)
        dag = MultiOutputNode([o3, o4])

    compiled_dag = dag.experimental_compile()
    for _ in range(3):
        refs = compiled_dag.execute(1)
        if single_fetch:
            for i, ref in enumerate(refs):
                res = ray.get(ref)
                assert res == i + 1
        else:
            res = ray.get(refs)
            assert res == [1, 2]


@pytest.mark.parametrize("single_fetch", [True, False])
def test_inc_two_returns(ray_start_regular, single_fetch):
    a = Actor.remote(0)
    with InputNode() as i:
        o1, o2 = a.inc_and_return_two.bind(i)
        dag = MultiOutputNode([o1, o2])

    compiled_dag = dag.experimental_compile()
    compiled_dag.visualize(channel_details=True)
    for i in range(3):
        refs = compiled_dag.execute(1)
        if single_fetch:
            for j, ref in enumerate(refs):
                res = ray.get(ref)
                assert res == i + j + 1
        else:
            res = ray.get(refs)
            assert res == [i + 1, i + 2]


def test_two_as_one_return(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as i:
        o1 = a.return_two_as_one.bind(i)
        dag = o1

    compiled_dag = dag.experimental_compile()
    for _ in range(3):
        res = ray.get(compiled_dag.execute(1))
        assert res == (1, 2)


def test_multi_output_get_exception(ray_start_regular):
    a = Actor.remote(0)
    b = Actor.remote(0)
    with InputNode() as i:
        o1, o2 = a.return_two.bind(i)
        o3 = b.echo.bind(o1)
        o4 = b.echo.bind(o2)
        dag = MultiOutputNode([o3, o4])

    compiled_dag = dag.experimental_compile()
    refs = compiled_dag.execute(1)
    refs.append(None)

    with pytest.raises(
        ValueError,
        match="Invalid type of object refs. 'object_refs' must be a list of "
        "CompiledDAGRefs if there is any CompiledDAGRef within it.",
    ):
        ray.get(refs)


def test_two_from_three_returns(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as i:
        o1, o2 = a.return_two_from_three.bind(i)
        dag = MultiOutputNode([o1, o2])

    compiled_dag = dag.experimental_compile()

    # A value error is raised because the number of returns is not equal to
    # the number of outputs. Since the value error is raised in the writer,
    # the reader fails to read the outputs and raises a channel error.

    # TODO(wxdeng): Fix exception type. The value error should be catched.
    # However, two exceptions are raised in the writer and reader respectively.

    # with pytest.raises(RayChannelError, match="Channel closed."):
    # with pytest.raises(ValueError, match="Expected 2 outputs, but got 3 outputs"):
    with pytest.raises(Exception):
        ray.get(compiled_dag.execute(1))


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


def test_actor_multi_methods(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)
        dag = a.echo.bind(dag)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(1)
    result = ray.get(ref)
    assert result == 1


@pytest.mark.parametrize("single_fetch", [True, False])
def test_actor_methods_execution_order(ray_start_regular, single_fetch):
    actor1 = Actor.remote(0)
    actor2 = Actor.remote(0)
    with InputNode() as inp:
        branch1 = actor1.inc.bind(inp)
        branch1 = actor2.double_and_inc.bind(branch1)
        branch2 = actor2.inc.bind(inp)
        branch2 = actor1.double_and_inc.bind(branch2)
        dag = MultiOutputNode([branch2, branch1])

    compiled_dag = dag.experimental_compile()
    refs = compiled_dag.execute(1)
    # test that double_and_inc() is called after inc() on actor1
    if single_fetch:
        assert ray.get(refs[0]) == 4
        assert ray.get(refs[1]) == 1
    else:
        assert ray.get(refs) == [4, 1]


def test_actor_method_multi_binds(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)
        dag = a.inc.bind(dag)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(1)
    result = ray.get(ref)
    assert result == 2


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


def test_actor_method_bind_same_input(ray_start_regular):
    actor = Actor.remote(0)
    with InputNode() as inp:
        # Test binding input node to the same method
        # of same actor multiple times: execution
        # should not hang.
        output1 = actor.inc.bind(inp)
        output2 = actor.inc.bind(inp)
        dag = MultiOutputNode([output1, output2])
    compiled_dag = dag.experimental_compile()
    expected = [[0, 0], [1, 2], [4, 6]]
    for i in range(3):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == expected[i]


def test_actor_method_bind_same_input_attr(ray_start_regular):
    actor = Actor.remote(0)
    with InputNode() as inp:
        # Test binding input attribute node to the same method
        # of same actor multiple times: execution should not
        # hang.
        output1 = actor.inc.bind(inp[0])
        output2 = actor.inc.bind(inp[0])
        dag = MultiOutputNode([output1, output2])
    compiled_dag = dag.experimental_compile()
    expected = [[0, 0], [1, 2], [4, 6]]
    for i in range(3):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == expected[i]


def test_actor_method_bind_diff_input_attr_1(ray_start_regular):
    actor = Actor.remote(0)
    c = Collector.remote()
    with InputNode() as inp:
        # Two class methods are bound to two different input
        # attribute nodes.
        branch1 = actor.inc.bind(inp[0])
        branch2 = actor.inc.bind(inp[1])
        dag = c.collect_two.bind(branch1, branch2)
    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(0, 1)
    assert ray.get(ref) == [0, 1]

    ref = compiled_dag.execute(1, 2)
    assert ray.get(ref) == [0, 1, 2, 4]

    ref = compiled_dag.execute(2, 3)
    assert ray.get(ref) == [0, 1, 2, 4, 6, 9]


def test_actor_method_bind_diff_input_attr_2(ray_start_regular):
    actor = Actor.remote(0)
    c = Collector.remote()
    with InputNode() as inp:
        # Three class methods are bound to two different input
        # attribute nodes. Two methods are bound to the same input
        # attribute node.
        branch1 = actor.inc.bind(inp[0])
        branch2 = actor.inc.bind(inp[0])
        branch3 = actor.inc.bind(inp[1])
        dag = c.collect_three.bind(branch1, branch2, branch3)
    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(0, 1)
    assert ray.get(ref) == [0, 0, 1]

    ref = compiled_dag.execute(1, 2)
    assert ray.get(ref) == [0, 0, 1, 2, 3, 5]

    ref = compiled_dag.execute(2, 3)
    assert ray.get(ref) == [0, 0, 1, 2, 3, 5, 7, 9, 12]


def test_actor_method_bind_diff_input_attr_3(ray_start_regular):
    actor = Actor.remote(0)
    with InputNode() as inp:
        # A single class method is bound to two different input
        # attribute nodes.
        dag = actor.inc_two.bind(inp[0], inp[1])
    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(0, 1)
    assert ray.get(ref) == 1

    ref = compiled_dag.execute(1, 2)
    assert ray.get(ref) == 4

    ref = compiled_dag.execute(2, 3)
    assert ray.get(ref) == 9


class TestDAGNodeInsideContainer:
    regex = r"Found \d+ DAGNodes from the arg .*? in .*?\.\s*"
    r"Please ensure that the argument is a single DAGNode and that a "
    r"DAGNode is not allowed to be placed inside any type of container\."

    def test_dag_node_in_list(self, ray_start_regular):
        actor = Actor.remote(0)
        with pytest.raises(ValueError) as exc_info:
            with InputNode() as inp:
                dag = actor.echo.bind([inp])
            dag.experimental_compile()
        assert re.search(self.regex, str(exc_info.value), re.DOTALL)

    def test_dag_node_in_tuple(self, ray_start_regular):
        actor = Actor.remote(0)
        with pytest.raises(ValueError) as exc_info:
            with InputNode() as inp:
                dag = actor.echo.bind((inp,))
            dag.experimental_compile()
        assert re.search(self.regex, str(exc_info.value), re.DOTALL)

    def test_dag_node_in_dict(self, ray_start_regular):
        actor = Actor.remote(0)
        with pytest.raises(ValueError) as exc_info:
            with InputNode() as inp:
                dag = actor.echo.bind({"inp": inp})
            dag.experimental_compile()
        assert re.search(self.regex, str(exc_info.value), re.DOTALL)

    def test_two_dag_nodes_in_list(self, ray_start_regular):
        actor = Actor.remote(0)
        with pytest.raises(ValueError) as exc_info:
            with InputNode() as inp:
                dag = actor.echo.bind([inp, inp])
            dag.experimental_compile()
        assert re.search(self.regex, str(exc_info.value), re.DOTALL)

    def test_dag_node_in_class(self, ray_start_regular):
        class OuterClass:
            def __init__(self, ref):
                self.ref = ref

        actor = Actor.remote(0)
        with pytest.raises(ValueError) as exc_info:
            with InputNode() as inp:
                dag = actor.echo.bind(OuterClass(inp))
            dag.experimental_compile()
        assert re.search(self.regex, str(exc_info.value), re.DOTALL)


def test_actor_method_bind_diff_input_attr_4(ray_start_regular):
    actor = Actor.remote(0)
    c = Collector.remote()
    with InputNode() as inp:
        branch1 = actor.inc_two.bind(inp[0], inp[1])
        branch2 = actor.inc.bind(inp[2])
        dag = c.collect_two.bind(branch1, branch2)
    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(0, 1, 2)
    assert ray.get(ref) == [1, 3]

    ref = compiled_dag.execute(1, 2, 3)
    assert ray.get(ref) == [1, 3, 6, 9]

    ref = compiled_dag.execute(2, 3, 4)
    assert ray.get(ref) == [1, 3, 6, 9, 14, 18]


def test_actor_method_bind_diff_input_attr_5(ray_start_regular):
    actor = Actor.remote(0)
    c = Collector.remote()
    with InputNode() as inp:
        branch1 = actor.inc_two.bind(inp[0], inp[1])
        branch2 = actor.inc_two.bind(inp[2], inp[0])
        dag = c.collect_two.bind(branch1, branch2)
    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(0, 1, 2)
    assert ray.get(ref) == [1, 3]

    ref = compiled_dag.execute(1, 2, 3)
    assert ray.get(ref) == [1, 3, 6, 10]

    ref = compiled_dag.execute(2, 3, 4)
    assert ray.get(ref) == [1, 3, 6, 10, 15, 21]


def test_actor_method_bind_diff_kwargs_input_attr(ray_start_regular):
    actor = Actor.remote(0)
    c = Collector.remote()
    with InputNode() as inp:
        # Two class methods are bound to two different kwargs input
        # attribute nodes.
        branch1 = actor.inc.bind(inp.x)
        branch2 = actor.inc.bind(inp.y)
        dag = c.collect_two.bind(branch1, branch2)
    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(x=0, y=1)
    assert ray.get(ref) == [0, 1]

    ref = compiled_dag.execute(x=1, y=2)
    assert ray.get(ref) == [0, 1, 2, 4]

    ref = compiled_dag.execute(x=2, y=3)
    assert ray.get(ref) == [0, 1, 2, 4, 6, 9]


def test_actor_method_bind_same_arg(ray_start_regular):
    a1 = Actor.remote(0)
    a2 = Actor.remote(0)
    with InputNode() as inp:
        # Test binding arg to the same method
        # of same actor multiple times: execution
        # should not hang.
        output1 = a1.echo.bind(inp)
        output2 = a2.inc.bind(output1)
        output3 = a2.inc.bind(output1)
        dag = MultiOutputNode([output2, output3])
    compiled_dag = dag.experimental_compile()
    expected = [[0, 0], [1, 2], [4, 6]]
    for i in range(3):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == expected[i]


def test_mixed_bind_same_input(ray_start_regular):
    a1 = Actor.remote(0)
    a2 = Actor.remote(0)
    with InputNode() as inp:
        # Test binding input node to the same method
        # of different actors multiple times: execution
        # should not hang.
        output1 = a1.inc.bind(inp)
        output2 = a1.inc.bind(inp)
        output3 = a2.inc.bind(inp)
        dag = MultiOutputNode([output1, output2, output3])
    compiled_dag = dag.experimental_compile()
    expected = [[0, 0, 0], [1, 2, 1], [4, 6, 3]]
    for i in range(3):
        ref = compiled_dag.execute(i)
        result = ray.get(ref)
        assert result == expected[i]


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


class TestMultiArgs:
    def test_multi_args_basic(self, ray_start_regular):
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

    def test_multi_args_single_actor(self, ray_start_regular):
        c = Collector.remote()
        with InputNode() as i:
            dag = c.collect_three.bind(i[0], i[1], i[0])

        compiled_dag = dag.experimental_compile()

        expected = [[0, 1, 0], [0, 1, 0, 1, 2, 1], [0, 1, 0, 1, 2, 1, 2, 3, 2]]
        for i in range(3):
            ref = compiled_dag.execute(i, i + 1)
            result = ray.get(ref)
            assert result == expected[i]

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

    def test_multi_args_branch(self, ray_start_regular):
        a = Actor.remote(0)
        c = Collector.remote()
        with InputNode() as i:
            branch = a.inc.bind(i[0])
            dag = c.collect_two.bind(branch, i[1])

        compiled_dag = dag.experimental_compile()

        ref = compiled_dag.execute(2, 3)
        result = ray.get(ref)
        assert result == [2, 3]

    def test_kwargs_basic(self, ray_start_regular):
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

    def test_kwargs_single_actor(self, ray_start_regular):
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
            match=r"dag.execute\(\) or dag.execute_async\(\) "
            "must be called with kwarg `x`",
        ):
            compiled_dag.execute(y=3)

        with pytest.raises(
            ValueError,
            match=r"dag.execute\(\) or dag.execute_async\(\) "
            "must be called with kwarg `y`",
        ):
            compiled_dag.execute(x=3)

    def test_kwargs_branch(self, ray_start_regular):
        a = Actor.remote(0)
        c = Collector.remote()
        with InputNode() as i:
            branch = a.inc.bind(i.x)
            dag = c.collect_two.bind(i.y, branch)

        compiled_dag = dag.experimental_compile()

        ref = compiled_dag.execute(x=2, y=3)
        result = ray.get(ref)
        assert result == [3, 2]

    def test_multi_args_and_kwargs(self, ray_start_regular):
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

    def test_multi_args_and_torch_type(self, ray_start_regular):
        a1 = Actor.remote(0)
        a2 = Actor.remote(0)
        c = Collector.remote()
        with InputNode() as i:
            i.with_tensor_transport()
            branch1 = a1.echo.bind(i[0])
            branch1.with_tensor_transport()
            branch2 = a2.echo.bind(i[1])
            branch2.with_tensor_transport()
            dag = c.collect_two.bind(branch2, branch1)
            dag.with_tensor_transport()

        compiled_dag = dag.experimental_compile()

        cpu_tensors = [torch.tensor([0, 0, 0, 0, 0]), torch.tensor([1, 1, 1, 1, 1])]
        ref = compiled_dag.execute(cpu_tensors[0], cpu_tensors[1])

        tensors = ray.get(ref)
        assert len(tensors) == len(cpu_tensors)
        assert torch.equal(tensors[0], cpu_tensors[1])
        assert torch.equal(tensors[1], cpu_tensors[0])

    def test_mix_entire_input_and_args(self, ray_start_regular):
        """
        It is not allowed to consume both the entire input and a partial
        input (i.e., an InputAttributeNode) as arguments.
        """
        a = Actor.remote(0)
        c = Collector.remote()
        with InputNode() as i:
            branch = a.inc_two.bind(i[0], i[1])
            dag = c.collect_two.bind(i, branch)

        with pytest.raises(
            ValueError,
            match=re.escape(
                "All tasks must either use InputNode() directly, "
                "or they must index to specific args or kwargs."
            ),
        ):
            dag.experimental_compile()

    def test_multi_args_same_actor(self, ray_start_regular):
        a1 = Actor.remote(0)
        with InputNode() as i:
            branch1 = a1.inc.bind(i[0])
            branch2 = a1.inc.bind(i[1])
            dag = MultiOutputNode([branch1, branch2])

        compiled_dag = dag.experimental_compile()

        ref = compiled_dag.execute(1, 2)
        result = ray.get(ref)
        assert result == [1, 3]

    def test_multi_args_basic_asyncio(self, ray_start_regular):
        a1 = Actor.remote(0)
        a2 = Actor.remote(0)
        c = Collector.remote()
        with InputNode() as i:
            branch1 = a1.inc.bind(i[0])
            branch2 = a2.inc.bind(i[1])
            dag = c.collect_two.bind(branch2, branch1)
        compiled_dag = dag.experimental_compile(enable_asyncio=True)

        async def main():
            fut = await compiled_dag.execute_async(2, 3)
            result = await fut
            assert result == [3, 2]

        loop = get_or_create_event_loop()
        loop.run_until_complete(asyncio.gather(main()))

    def test_multi_args_branch_asyncio(self, ray_start_regular):
        a = Actor.remote(0)
        c = Collector.remote()
        with InputNode() as i:
            branch = a.inc.bind(i[0])
            dag = c.collect_two.bind(branch, i[1])

        compiled_dag = dag.experimental_compile(enable_asyncio=True)

        async def main():
            fut = await compiled_dag.execute_async(2, 3)
            result = await fut
            assert result == [2, 3]

        loop = get_or_create_event_loop()
        loop.run_until_complete(asyncio.gather(main()))

    def test_kwargs_basic_asyncio(self, ray_start_regular):
        a1 = Actor.remote(0)
        a2 = Actor.remote(0)
        c = Collector.remote()
        with InputNode() as i:
            branch1 = a1.inc.bind(i.x)
            branch2 = a2.inc.bind(i.y)
            dag = c.collect_two.bind(branch2, branch1)

        compiled_dag = dag.experimental_compile(enable_asyncio=True)

        async def main():
            fut = await compiled_dag.execute_async(x=2, y=3)
            result = await fut
            assert result == [3, 2]

        loop = get_or_create_event_loop()
        loop.run_until_complete(asyncio.gather(main()))

    def test_kwargs_branch_asyncio(self, ray_start_regular):
        a = Actor.remote(0)
        c = Collector.remote()
        with InputNode() as i:
            branch = a.inc.bind(i.x)
            dag = c.collect_two.bind(i.y, branch)

        compiled_dag = dag.experimental_compile(enable_asyncio=True)

        async def main():
            fut = await compiled_dag.execute_async(x=2, y=3)
            result = await fut
            assert result == [3, 2]

        loop = get_or_create_event_loop()
        loop.run_until_complete(asyncio.gather(main()))

    def test_multi_args_and_kwargs_asyncio(self, ray_start_regular):
        a1 = Actor.remote(0)
        a2 = Actor.remote(0)
        c = Collector.remote()
        with InputNode() as i:
            branch1 = a1.inc.bind(i[0])
            branch2 = a2.inc.bind(i.y)
            dag = c.collect_three.bind(branch2, i.z, branch1)

        compiled_dag = dag.experimental_compile(enable_asyncio=True)

        async def main():
            fut = await compiled_dag.execute_async(2, y=3, z=4)
            result = await fut
            assert result == [3, 4, 2]

        loop = get_or_create_event_loop()
        loop.run_until_complete(asyncio.gather(main()))


@pytest.mark.parametrize("num_actors", [1, 4])
@pytest.mark.parametrize("single_fetch", [True, False])
def test_scatter_gather_dag(ray_start_regular, num_actors, single_fetch):
    actors = [Actor.remote(0) for _ in range(num_actors)]
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = MultiOutputNode(out)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        refs = compiled_dag.execute(1)
        if single_fetch:
            assert isinstance(refs, list)
            for j in range(num_actors):
                result = ray.get(refs[j])
                assert result == i + 1
        else:
            results = ray.get(refs)
            assert results == [i + 1] * num_actors


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


def test_get_with_zero_timeout(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.inc.bind(inp)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(1)
    # Give enough time for DAG execution result to be ready
    time.sleep(2)
    # Use timeout=0 to either get result immediately or raise an exception
    result = ray.get(ref, timeout=0)
    assert result == 1


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


@pytest.mark.parametrize("single_fetch", [True, False])
def test_dag_exception_multi_output(ray_start_regular, single_fetch, capsys):
    # Test application throwing exceptions with a DAG with multiple outputs.
    a = Actor.remote(0)
    b = Actor.remote(0)
    with InputNode() as inp:
        dag = MultiOutputNode([a.inc.bind(inp), b.inc.bind(inp)])

    compiled_dag = dag.experimental_compile()

    # Can throw an error.
    refs = compiled_dag.execute("hello")
    if single_fetch:
        for ref in refs:
            with pytest.raises(TypeError) as exc_info:
                ray.get(ref)
            # Traceback should match the original actor class definition.
            assert "self.i += x" in str(exc_info.value)
    else:
        with pytest.raises(TypeError) as exc_info:
            ray.get(refs)
        # Traceback should match the original actor class definition.
        assert "self.i += x" in str(exc_info.value)

    # Can throw an error multiple times.
    refs = compiled_dag.execute("hello")
    if single_fetch:
        for ref in refs:
            with pytest.raises(TypeError) as exc_info:
                ray.get(ref)
            # Traceback should match the original actor class definition.
            assert "self.i += x" in str(exc_info.value)
    else:
        with pytest.raises(TypeError) as exc_info:
            ray.get(refs)
        # Traceback should match the original actor class definition.
        assert "self.i += x" in str(exc_info.value)

    # Can use the DAG after exceptions are thrown.
    refs = compiled_dag.execute(1)
    if single_fetch:
        assert ray.get(refs[0]) == 1
        assert ray.get(refs[1]) == 1
    else:
        assert ray.get(refs) == [1, 1]


def test_dag_errors(ray_start_regular):
    a = Actor.remote(0)
    dag = a.inc.bind(1)
    with pytest.raises(
        ValueError,
        match="No InputNode found in the DAG: when traversing upwards, "
        "no upstream node was found for",
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
            r"ray.get\(\) can only be called once "
            r"on a CompiledDAGRef, and it was already called."
        ),
    ):
        ray.get(ref)


class TestDAGExceptionCompileMultipleTimes:
    def test_compile_twice_with_teardown(self, ray_start_regular):
        a = Actor.remote(0)
        with InputNode() as i:
            dag = a.echo.bind(i)
        compiled_dag = dag.experimental_compile()
        compiled_dag.teardown()
        with pytest.raises(
            ValueError,
            match="It is not allowed to call `experimental_compile` on the same DAG "
            "object multiple times no matter whether `teardown` is called or not. "
            "Please reuse the existing compiled DAG or create a new one.",
        ):
            compiled_dag = dag.experimental_compile()

    def test_compile_twice_without_teardown(self, ray_start_regular):
        a = Actor.remote(0)
        with InputNode() as i:
            dag = a.echo.bind(i)
        compiled_dag = dag.experimental_compile()
        with pytest.raises(
            ValueError,
            match="It is not allowed to call `experimental_compile` on the same DAG "
            "object multiple times no matter whether `teardown` is called or not. "
            "Please reuse the existing compiled DAG or create a new one.",
        ):
            compiled_dag = dag.experimental_compile()
        compiled_dag.teardown()

    def test_compile_twice_with_multioutputnode(self, ray_start_regular):
        a = Actor.remote(0)
        with InputNode() as i:
            dag = MultiOutputNode([a.echo.bind(i)])
        compiled_dag = dag.experimental_compile()
        compiled_dag.teardown()
        with pytest.raises(
            ValueError,
            match="It is not allowed to call `experimental_compile` on the same DAG "
            "object multiple times no matter whether `teardown` is called or not. "
            "Please reuse the existing compiled DAG or create a new one.",
        ):
            compiled_dag = dag.experimental_compile()

    def test_compile_twice_with_multioutputnode_without_teardown(
        self, ray_start_regular
    ):
        a = Actor.remote(0)
        with InputNode() as i:
            dag = MultiOutputNode([a.echo.bind(i)])
        compiled_dag = dag.experimental_compile()
        with pytest.raises(
            ValueError,
            match="It is not allowed to call `experimental_compile` on the same DAG "
            "object multiple times no matter whether `teardown` is called or not. "
            "Please reuse the existing compiled DAG or create a new one.",
        ):
            compiled_dag = dag.experimental_compile()  # noqa

    def test_compile_twice_with_different_nodes(self, ray_start_regular):
        a = Actor.remote(0)
        b = Actor.remote(0)
        with InputNode() as i:
            branch1 = a.echo.bind(i)
            branch2 = b.echo.bind(i)
            dag = MultiOutputNode([branch1, branch2])
        compiled_dag = dag.experimental_compile()
        compiled_dag.teardown()
        with pytest.raises(
            ValueError,
            match="The DAG was compiled more than once. The following two "
            "nodes call `experimental_compile`: ",
        ):
            branch2.experimental_compile()


def test_exceed_max_buffered_results(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as i:
        dag = a.inc.bind(i)

    compiled_dag = dag.experimental_compile(_max_buffered_results=1)

    refs = []
    for i in range(2):
        ref = compiled_dag.execute(1)
        # Hold the refs to avoid get() being called on the ref
        # when it goes out of scope
        refs.append(ref)

    # ray.get() on the 2nd ref fails because the DAG cannot buffer 2 results.
    with pytest.raises(
        ray.exceptions.RayCgraphCapacityExceeded,
        match=(
            "The compiled graph can't have more than 1 buffered results, "
            r"and you currently have 1 buffered results. Call `ray.get\(\)` on "
            r"CompiledDAGRef's \(or await on CompiledDAGFuture's\) to retrieve "
            "results, or increase `_max_buffered_results` if buffering is "
            "desired, note that this will increase driver memory usage."
        ),
    ):
        ray.get(ref)

    del refs


@pytest.mark.parametrize("single_fetch", [True, False])
def test_exceed_max_buffered_results_multi_output(ray_start_regular, single_fetch):
    a = Actor.remote(0)
    b = Actor.remote(0)
    with InputNode() as inp:
        dag = MultiOutputNode([a.inc.bind(inp), b.inc.bind(inp)])

    compiled_dag = dag.experimental_compile(_max_buffered_results=1)

    refs = []
    for _ in range(2):
        ref = compiled_dag.execute(1)
        # Hold the refs to avoid get() being called on the ref
        # when it goes out of scope
        refs.append(ref)

    if single_fetch:
        # If there are results not fetched from an execution, that execution
        # still counts towards the number of buffered results.
        ray.get(refs[0][0])

    # ray.get() on the 2nd ref fails because the DAG cannot buffer 2 results.
    with pytest.raises(
        ray.exceptions.RayCgraphCapacityExceeded,
        match=(
            "The compiled graph can't have more than 1 buffered results, "
            r"and you currently have 1 buffered results. Call `ray.get\(\)` on "
            r"CompiledDAGRef's \(or await on CompiledDAGFuture's\) to retrieve "
            "results, or increase `_max_buffered_results` if buffering is "
            "desired, note that this will increase driver memory usage."
        ),
    ):
        if single_fetch:
            ray.get(ref[0])
        else:
            ray.get(ref)

    del refs


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


def test_dag_fault_tolerance_chain(ray_start_regular):
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


@pytest.mark.parametrize("single_fetch", [True, False])
def test_dag_fault_tolerance(ray_start_regular, single_fetch):
    actors = [
        Actor.remote(0, fail_after=100 if i == 0 else None, sys_exit=False)
        for i in range(4)
    ]
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = MultiOutputNode(out)

    compiled_dag = dag.experimental_compile()

    for i in range(99):
        refs = compiled_dag.execute(1)
        if single_fetch:
            for j in range(len(actors)):
                assert ray.get(refs[j]) == i + 1
        else:
            assert ray.get(refs) == [i + 1] * len(actors)

    with pytest.raises(RuntimeError):
        for i in range(99, 200):
            refs = compiled_dag.execute(1)
            if single_fetch:
                for j in range(len(actors)):
                    assert ray.get(refs[j]) == i + 1
            else:
                assert ray.get(refs) == [i + 1] * len(actors)

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
        refs = compiled_dag.execute(1)
        if single_fetch:
            for j in range(len(actors)):
                ray.get(refs[j])
        else:
            ray.get(refs)


@pytest.mark.parametrize("single_fetch", [True, False])
def test_dag_fault_tolerance_sys_exit(ray_start_regular, single_fetch):
    actors = [
        Actor.remote(0, fail_after=100 if i == 0 else None, sys_exit=True)
        for i in range(4)
    ]
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = MultiOutputNode(out)

    compiled_dag = dag.experimental_compile()

    for i in range(99):
        refs = compiled_dag.execute(1)
        if single_fetch:
            for j in range(len(actors)):
                assert ray.get(refs[j]) == i + 1
        else:
            assert ray.get(refs) == [i + 1] * len(actors)

    with pytest.raises(
        ActorDiedError, match="The actor died unexpectedly before finishing this task."
    ):
        for i in range(99):
            refs = compiled_dag.execute(1)
            if single_fetch:
                for j in range(len(actors)):
                    ray.get(refs[j])
            else:
                ray.get(refs)

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
        refs = compiled_dag.execute(1)
        if single_fetch:
            for j in range(len(actors)):
                ray.get(refs[j])
        else:
            ray.get(refs)


def test_dag_teardown_while_running(ray_start_regular):
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


def test_asyncio(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as i:
        dag = a.echo.bind(i)

    loop = get_or_create_event_loop()
    compiled_dag = dag.experimental_compile(enable_asyncio=True)

    async def main(i):
        # Use numpy so that the return value will be zero-copy deserialized. If
        # there is a memory leak in the DAG backend, then only the first task
        # will succeed.
        val = np.ones(100) * i
        fut = await compiled_dag.execute_async(val)
        result = await fut
        assert (result == val).all()

    loop.run_until_complete(asyncio.gather(*[main(i) for i in range(10)]))


def test_asyncio_out_of_order_get(ray_start_regular):
    c = Collector.remote()
    with InputNode() as i:
        dag = c.collect.bind(i)

    loop = get_or_create_event_loop()
    compiled_dag = dag.experimental_compile(enable_asyncio=True)

    async def main():
        fut_a = await compiled_dag.execute_async("a")
        fut_b = await compiled_dag.execute_async("b")

        result_b = await fut_b
        assert result_b == ["a", "b"]
        result_a = await fut_a
        assert result_a == ["a"]

    loop.run_until_complete(main())


@pytest.mark.parametrize("gather_futs", [True, False])
def test_asyncio_multi_output(ray_start_regular, gather_futs):
    a = Actor.remote(0)
    b = Actor.remote(0)
    with InputNode() as i:
        dag = MultiOutputNode([a.echo.bind(i), b.echo.bind(i)])

    loop = get_or_create_event_loop()
    compiled_dag = dag.experimental_compile(enable_asyncio=True)

    async def main(i):
        # Use numpy so that the return value will be zero-copy deserialized. If
        # there is a memory leak in the DAG backend, then only the first task
        # will succeed.
        val = np.ones(100) * i
        futs = await compiled_dag.execute_async(val)
        assert len(futs) == 2

        if gather_futs:
            results = await asyncio.gather(*futs)
            assert len(results) == 2
            for result in results:
                assert (result == val).all()
        else:
            for fut in futs:
                result = await fut
                assert (result == val).all()

    loop.run_until_complete(asyncio.gather(*[main(i) for i in range(10)]))


def test_asyncio_exceptions(ray_start_regular):
    a = Actor.remote(0)
    with InputNode() as i:
        dag = a.inc.bind(i)

    loop = get_or_create_event_loop()
    compiled_dag = dag.experimental_compile(enable_asyncio=True)

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


class TestCompositeChannel:
    def test_composite_channel_one_actor(self, ray_start_regular):
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

    def test_composite_channel_two_actors(self, ray_start_regular):
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

    @pytest.mark.parametrize("single_fetch", [True, False])
    def test_composite_channel_multi_output(self, ray_start_regular, single_fetch):
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
        refs = compiled_dag.execute(1)
        if single_fetch:
            assert ray.get(refs[0]) == 2
            assert ray.get(refs[1]) == 101
        else:
            assert ray.get(refs) == [2, 101]

        refs = compiled_dag.execute(3)
        if single_fetch:
            assert ray.get(refs[0]) == 10
            assert ray.get(refs[1]) == 106
        else:
            assert ray.get(refs) == [10, 106]

    @pytest.mark.parametrize("single_fetch", [True, False])
    def test_intra_process_channel_with_multi_readers(
        self, ray_start_regular, single_fetch
    ):
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
        refs = compiled_dag.execute(1)
        if single_fetch:
            assert ray.get(refs[0]) == 1
            assert ray.get(refs[1]) == 1
        else:
            assert ray.get(refs) == [1, 1]

        refs = compiled_dag.execute(2)
        if single_fetch:
            assert ray.get(refs[0]) == 2
            assert ray.get(refs[1]) == 2
        else:
            assert ray.get(refs) == [2, 2]

        refs = compiled_dag.execute(3)
        if single_fetch:
            assert ray.get(refs[0]) == 3
            assert ray.get(refs[1]) == 3
        else:
            assert ray.get(refs) == [3, 3]


@ray.remote
class FastFailActor:
    def sleep_and_echo(self, x):
        time.sleep(x)
        return x

    def fail_if_x_is_even(self, x):
        if x % 2 == 0:
            raise ValueError("x is even")
        return x

    def sleep_and_fail(self, x):
        time.sleep(x)
        raise ValueError("fail")


class TestFastFail:
    @pytest.mark.parametrize("is_async", [True, False])
    def test_first_input_fail(self, ray_start_regular, is_async):
        """
        Tests the case where the failing input is at the beginning of the input list.
        """
        a = FastFailActor.remote()
        with InputNode() as inp:
            dag = MultiOutputNode(
                [a.fail_if_x_is_even.bind(inp), a.sleep_and_echo.bind(inp)]
            )
        compiled_dag = dag.experimental_compile(enable_asyncio=is_async)

        if is_async:

            async def main():
                futs = await compiled_dag.execute_async(6)
                start_time = time.time()
                with pytest.raises(ValueError, match="x is even"):
                    await asyncio.gather(*futs)
                end_time = time.time()
                assert end_time - start_time < 6

            loop = get_or_create_event_loop()
            loop.run_until_complete(main())
        else:
            start_time = time.time()
            with pytest.raises(ValueError, match="x is even"):
                ray.get(compiled_dag.execute(6))
            end_time = time.time()
            assert end_time - start_time < 6

    @pytest.mark.parametrize("is_async", [True, False])
    def test_last_input_fail(self, ray_start_regular, is_async):
        """
        Tests the case where the failing input is at the end of the input list.
        The test cannot use the same actor for both `sleep_and_echo` and
        `fail_if_x_is_even` tasks because the control dependency would make the
        `fail_if_x_is_even` task execute after the `sleep_and_echo` task finishes.
        """
        a = FastFailActor.remote()
        b = FastFailActor.remote()
        with InputNode() as inp:
            dag = MultiOutputNode(
                [a.sleep_and_echo.bind(inp), b.fail_if_x_is_even.bind(inp)]
            )
        compiled_dag = dag.experimental_compile(enable_asyncio=is_async)

        if is_async:

            async def main():
                futs = await compiled_dag.execute_async(6)
                start_time = time.time()
                with pytest.raises(ValueError, match="x is even"):
                    await asyncio.gather(*futs)
                end_time = time.time()
                assert end_time - start_time < 6

            loop = get_or_create_event_loop()
            loop.run_until_complete(main())
        else:
            start_time = time.time()
            with pytest.raises(ValueError, match="x is even"):
                ray.get(compiled_dag.execute(6))
            end_time = time.time()
            assert end_time - start_time < 6

    @pytest.mark.parametrize("is_async", [True, False])
    def test_get_one_of_output_refs(self, ray_start_regular, is_async):
        """
        Tests the case where `ray.get` is called on only one of the output refs
        which doesn't fail.
        """
        a = FastFailActor.remote()
        b = FastFailActor.remote()
        with InputNode() as inp:
            dag = MultiOutputNode(
                [a.sleep_and_echo.bind(inp), b.fail_if_x_is_even.bind(inp)]
            )
        compiled_dag = dag.experimental_compile(enable_asyncio=is_async)

        if is_async:

            async def main():
                futs = await compiled_dag.execute_async(6)
                start_time = time.time()
                with pytest.raises(ValueError, match="x is even"):
                    await asyncio.gather(futs[0])
                end_time = time.time()
                assert end_time - start_time < 6

            loop = get_or_create_event_loop()
            loop.run_until_complete(main())
        else:
            start_time = time.time()
            with pytest.raises(ValueError, match="x is even"):
                refs = compiled_dag.execute(6)
                ray.get(refs[0])
            end_time = time.time()
            assert end_time - start_time < 6

    @pytest.mark.parametrize("is_async", [True, False])
    def test_middle_input_fail(self, ray_start_regular, is_async):
        """
        Tests the case where the failing input is in the middle of the input list.
        """
        a = FastFailActor.remote()
        b = FastFailActor.remote()
        c = FastFailActor.remote()
        with InputNode() as inp:
            dag = MultiOutputNode(
                [
                    a.sleep_and_echo.bind(inp),
                    b.fail_if_x_is_even.bind(inp),
                    c.sleep_and_echo.bind(inp),
                ]
            )
        compiled_dag = dag.experimental_compile(enable_asyncio=is_async)

        if is_async:

            async def main():
                futs = await compiled_dag.execute_async(6)
                start_time = time.time()
                with pytest.raises(ValueError, match="x is even"):
                    await asyncio.gather(*futs)
                end_time = time.time()
                assert end_time - start_time < 6

            loop = get_or_create_event_loop()
            loop.run_until_complete(main())
        else:
            start_time = time.time()
            with pytest.raises(ValueError, match="x is even"):
                ray.get(compiled_dag.execute(6))
            end_time = time.time()
            assert end_time - start_time < 6

    @pytest.mark.parametrize("is_async", [True, False])
    def test_all_inputs_fail(self, ray_start_regular, is_async):
        """
        Tests the case where all inputs fail with different sleep times.
        """
        a = FastFailActor.remote()
        b = FastFailActor.remote()
        c = FastFailActor.remote()
        with InputNode() as inp:
            dag = MultiOutputNode(
                [
                    a.sleep_and_fail.bind(inp[0]),
                    b.sleep_and_fail.bind(inp[1]),
                    c.sleep_and_fail.bind(inp[2]),
                ]
            )
        compiled_dag = dag.experimental_compile(enable_asyncio=is_async)

        if is_async:

            async def main():
                futs = await compiled_dag.execute_async(6, 0, 6)
                start_time = time.time()
                with pytest.raises(ValueError, match="fail"):
                    await asyncio.gather(*futs)
                end_time = time.time()
                assert end_time - start_time < 6

            loop = get_or_create_event_loop()
            loop.run_until_complete(main())
        else:
            start_time = time.time()
            with pytest.raises(ValueError, match="fail"):
                ray.get(compiled_dag.execute(6, 0, 6))
            end_time = time.time()
            assert end_time - start_time < 6

    @pytest.mark.parametrize("is_async", [True, False])
    def test_one_input_fail_and_retry_success(self, ray_start_regular, is_async):
        """
        Tests the case where only one input fails during the first execution, and
        subsequent executions succeed.
        """
        a = FastFailActor.remote()
        with InputNode() as inp:
            dag = MultiOutputNode(
                [a.fail_if_x_is_even.bind(inp), a.sleep_and_echo.bind(inp)]
            )
        compiled_dag = dag.experimental_compile(enable_asyncio=is_async)

        if is_async:

            async def main():
                futs = await compiled_dag.execute_async(2)
                with pytest.raises(ValueError, match="x is even"):
                    await asyncio.gather(*futs)
                for _ in range(3):
                    futs = await compiled_dag.execute_async(1)
                    assert await asyncio.gather(*futs) == [1, 1]

            loop = get_or_create_event_loop()
            loop.run_until_complete(main())
        else:
            with pytest.raises(ValueError, match="x is even"):
                ray.get(compiled_dag.execute(2))
            for _ in range(3):
                assert ray.get(compiled_dag.execute(1)) == [1, 1]

    @pytest.mark.parametrize("is_async", [True, False])
    def test_all_inputs_fail_and_retry_success(self, ray_start_regular, is_async):
        """
        Tests the case where all inputs fail during the first execution, but the
        subsequent executions succeed.
        """
        a = FastFailActor.remote()
        with InputNode() as inp:
            dag = MultiOutputNode(
                [a.fail_if_x_is_even.bind(inp), a.fail_if_x_is_even.bind(inp)]
            )
        compiled_dag = dag.experimental_compile(enable_asyncio=is_async)

        if is_async:

            async def main():
                futs = await compiled_dag.execute_async(2)
                with pytest.raises(ValueError, match="x is even"):
                    await asyncio.gather(*futs)
                for _ in range(3):
                    futs = await compiled_dag.execute_async(1)
                    assert await asyncio.gather(*futs) == [1, 1]

            loop = get_or_create_event_loop()
            loop.run_until_complete(main())
        else:
            with pytest.raises(ValueError, match="x is even"):
                ray.get(compiled_dag.execute(2))
            for _ in range(3):
                assert ray.get(compiled_dag.execute(1)) == [1, 1]

    def test_retry_timeout(self, ray_start_regular):
        """
        Tests the case where only one input fails during the first execution and
        subsequent executions timeout while consuming the leftover input.
        """
        a = FastFailActor.remote()
        with InputNode() as inp:
            dag = MultiOutputNode(
                [a.fail_if_x_is_even.bind(inp), a.sleep_and_echo.bind(inp)]
            )
        compiled_dag = dag.experimental_compile()

        with pytest.raises(ValueError, match="x is even"):
            ray.get(compiled_dag.execute(30))
        with pytest.raises(RayChannelTimeoutError):
            # RayChannelTimeoutError is raised when consuming the leftover input
            # which sleeps for 30 seconds.
            ref = compiled_dag.execute(1)
            ray.get(ref, timeout=3)


class TestLeafNode:
    """
    Leaf nodes are not allowed right now because the exception thrown by the leaf
    node will not be propagated to the driver and silently ignored, which is undesired.
    """

    LEAF_NODE_EXCEPTION_TEMPLATE = (
        "Compiled DAG doesn't support leaf nodes, i.e., nodes that don't have "
        "downstream nodes and are not output nodes. There are {num_leaf_nodes} "
        "leaf nodes in the DAG. Please add the outputs of"
    )

    def test_leaf_node_one_actor(self, ray_start_regular):
        """
        driver -> a.inc
               |
               -> a.inc -> driver
        """
        a = Actor.remote(0)
        with InputNode() as i:
            input_data = a.read_input.bind(i)
            a.inc.bind(input_data)  # branch1: leaf node
            branch2 = a.inc.bind(input_data)
            dag = MultiOutputNode([branch2])

        with pytest.raises(
            ValueError,
            match=TestLeafNode.LEAF_NODE_EXCEPTION_TEMPLATE.format(num_leaf_nodes=1),
        ):
            dag.experimental_compile()

    def test_leaf_node_two_actors(self, ray_start_regular):
        """
        driver -> b.inc -> a.inc --
               |        |         |
               |        -> b.inc ----> driver
               |
               -> a.inc (branch 1)
        """
        a = Actor.remote(0)
        b = Actor.remote(100)
        with InputNode() as i:
            a.inc.bind(i)  # branch1: leaf node
            branch2 = b.inc.bind(i)
            dag = MultiOutputNode([a.inc.bind(branch2), b.inc.bind(branch2)])
        with pytest.raises(
            ValueError,
            match=TestLeafNode.LEAF_NODE_EXCEPTION_TEMPLATE.format(num_leaf_nodes=1),
        ):
            dag.experimental_compile()

    def test_multi_leaf_nodes(self, ray_start_regular):
        """
        driver -> a.inc -> a.inc (branch 1, leaf node)
               |        |
               |        -> a.inc -> driver
               |
               -> a.inc (branch 2, leaf node)
        """
        a = Actor.remote(0)
        with InputNode() as i:
            dag = a.inc.bind(i)
            a.inc.bind(dag)  # branch1: leaf node
            a.inc.bind(i)  # branch2: leaf node
            dag = MultiOutputNode([a.inc.bind(dag)])

        with pytest.raises(
            ValueError,
            match=TestLeafNode.LEAF_NODE_EXCEPTION_TEMPLATE.format(num_leaf_nodes=2),
        ):
            dag.experimental_compile()

    def test_two_returns_first(self, ray_start_regular):
        a = Actor.remote(0)
        with InputNode() as i:
            o1, o2 = a.return_two.bind(i)
            dag = o1

        with pytest.raises(
            ValueError,
            match=TestLeafNode.LEAF_NODE_EXCEPTION_TEMPLATE.format(num_leaf_nodes=1),
        ):
            dag.experimental_compile()

    def test_two_returns_second(self, ray_start_regular):
        a = Actor.remote(0)
        with InputNode() as i:
            o1, o2 = a.return_two.bind(i)
            dag = o2
        with pytest.raises(
            ValueError,
            match=TestLeafNode.LEAF_NODE_EXCEPTION_TEMPLATE.format(num_leaf_nodes=1),
        ):
            dag.experimental_compile()


def test_output_node(ray_start_regular):
    """
    This test is similar to the `test_output_node` in `test_output_node.py`, but
    this test is for Compiled Graph.
    """

    @ray.remote
    class Worker:
        def __init__(self):
            pass

        def echo(self, data):
            return data

    worker1 = Worker.remote()
    worker2 = Worker.remote()
    worker3 = Worker.remote()
    with pytest.raises(ValueError):
        with InputNode() as input_data:
            dag = MultiOutputNode(worker1.echo.bind(input_data))

    with InputNode() as input_data:
        dag = MultiOutputNode([worker1.echo.bind(input_data)])
    compiled_dag = dag.experimental_compile()

    assert ray.get(compiled_dag.execute(1)) == [1]
    assert ray.get(compiled_dag.execute(2)) == [2]
    compiled_dag.teardown()

    with InputNode() as input_data:
        dag = MultiOutputNode(
            [worker1.echo.bind(input_data.x), worker2.echo.bind(input_data.y)]
        )
    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute(x=1, y=2)
    assert ray.get(ref) == [1, 2]
    compiled_dag.teardown()

    with InputNode() as input_data:
        dag = MultiOutputNode(
            [
                worker1.echo.bind(input_data.x),
                worker2.echo.bind(input_data.y),
                worker3.echo.bind(input_data.x),
            ]
        )
    compiled_dag = dag.experimental_compile()

    ref = compiled_dag.execute(x=1, y=2)
    assert ray.get(ref) == [1, 2, 1]


@pytest.mark.parametrize("single_fetch", [True, False])
def test_simulate_pipeline_parallelism(ray_start_regular, single_fetch):
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

    if single_fetch:
        assert ray.get(res[0]) == 0
        assert ray.get(res[1]) == 1
        assert ray.get(res[2]) == 2
    else:
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


def test_channel_read_after_close(ray_start_regular):
    # Tests that read to a channel after Compiled Graph teardown raises a
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


def test_channel_write_after_close(ray_start_regular):
    # Tests that write to a channel after Compiled Graph teardown raises a
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


def test_multiple_reads_from_same_actor(ray_start_cluster):
    a = Actor.remote(0)
    b = Actor.remote(10)
    with InputNode() as inp:
        x = a.inc.bind(inp)
        y = b.inc.bind(x)
        z = b.inc.bind(x)
        dag = MultiOutputNode([y, z])
    dag = dag.experimental_compile()
    assert ray.get(dag.execute(1)) == [11, 12]


def test_driver_and_actor_as_readers(ray_start_cluster):
    a = Actor.remote(0)
    b = Actor.remote(10)
    with InputNode() as inp:
        x = a.inc.bind(inp)
        y = b.inc.bind(x)
        dag = MultiOutputNode([x, y])
    dag = dag.experimental_compile()
    assert ray.get(dag.execute(1)) == [1, 11]


def test_driver_and_intraprocess_read(ray_start_cluster):
    """
    This test is similar to the `test_driver_and_actor_as_readers` test, but now for x,
    there is IntraProcessChannel to Actor a and a BufferedSharedMemoryChannel to the
    driver and the CompositeChannel has to choose the correct channel to read from in
    both situations.
    """
    a = Actor.remote(0)
    with InputNode() as inp:
        x = a.inc.bind(inp)
        y = a.inc.bind(x)
        dag = MultiOutputNode([x, y])
    dag = dag.experimental_compile()
    assert ray.get(dag.execute(1)) == [1, 2]


@pytest.mark.parametrize("temporary_change_timeout", [1], indirect=True)
def test_buffered_inputs(shutdown_only, temporary_change_timeout):
    ray.init()

    MAX_INFLIGHT_EXECUTIONS = 10
    DAG_EXECUTION_TIME = 0.2

    # Timeout should be larger than a single execution time.
    assert temporary_change_timeout > DAG_EXECUTION_TIME
    # Entire execution time (iteration * execution) should be higher than
    # the timeout for testing.
    assert DAG_EXECUTION_TIME * MAX_INFLIGHT_EXECUTIONS > temporary_change_timeout

    @ray.remote
    class Actor1:
        def fwd(self, x):
            print("Actor1 fwd")
            time.sleep(DAG_EXECUTION_TIME)
            return x

    actor1 = Actor1.remote()

    # Since the timeout is 1 second, if buffering is not working,
    # it will timeout (0.2s for each dag * MAX_INFLIGHT_EXECUTIONS).
    with InputNode() as input_node:
        dag = actor1.fwd.bind(input_node)

    # With buffering it should work.
    dag = dag.experimental_compile(_max_inflight_executions=MAX_INFLIGHT_EXECUTIONS)

    # Test the regular case.
    output_refs = []
    for i in range(MAX_INFLIGHT_EXECUTIONS):
        output_refs.append(dag.execute(i))
    for i, ref in enumerate(output_refs):
        assert ray.get(ref) == i

    # Test there are more items than max buffered inputs.
    output_refs = []
    for i in range(MAX_INFLIGHT_EXECUTIONS):
        output_refs.append(dag.execute(i))
    with pytest.raises(ray.exceptions.RayCgraphCapacityExceeded):
        dag.execute(1)
    assert len(output_refs) == MAX_INFLIGHT_EXECUTIONS
    for i, ref in enumerate(output_refs):
        assert ray.get(ref) == i

    # Make sure it works properly after that.
    output_refs = []
    for i in range(MAX_INFLIGHT_EXECUTIONS):
        output_refs.append(dag.execute(i))
    for i, ref in enumerate(output_refs):
        assert ray.get(ref) == i

    dag.teardown()

    # Test async case
    with InputNode() as input_node:
        async_dag = actor1.fwd.bind(input_node)

    async_dag = async_dag.experimental_compile(
        _max_inflight_executions=MAX_INFLIGHT_EXECUTIONS,
        enable_asyncio=True,
    )

    async def main():
        # Test the regular case.
        output_refs = []
        for i in range(MAX_INFLIGHT_EXECUTIONS):
            output_refs.append(await async_dag.execute_async(i))
        for i, ref in enumerate(output_refs):
            assert await ref == i

        # Test there are more items than max buffered inputs.
        output_refs = []
        for i in range(MAX_INFLIGHT_EXECUTIONS):
            output_refs.append(await async_dag.execute_async(i))
        with pytest.raises(ray.exceptions.RayCgraphCapacityExceeded):
            await async_dag.execute_async(1)
        assert len(output_refs) == MAX_INFLIGHT_EXECUTIONS
        for i, ref in enumerate(output_refs):
            assert await ref == i

        # Make sure it works properly after that.
        output_refs = []
        for i in range(MAX_INFLIGHT_EXECUTIONS):
            output_refs.append(await async_dag.execute_async(i))
        for i, ref in enumerate(output_refs):
            assert await ref == i

    loop = get_or_create_event_loop()
    loop.run_until_complete(main())


def test_inflight_requests_exceed_capacity(ray_start_regular):
    expected_error_message = (
        "The compiled graph can't have more than 2 "
        "in-flight executions, and you currently have 2 "
        "in-flight executions. Retrieve an output using ray.get before "
        "submitting more requests or increase `_max_inflight_executions`. "
    )
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.sleep.bind(inp)
    compiled_dag = dag.experimental_compile(_max_inflight_executions=2)
    ref1 = compiled_dag.execute(1)
    ref2 = compiled_dag.execute(1)
    with pytest.raises(
        ray.exceptions.RayCgraphCapacityExceeded,
        match=(expected_error_message),
    ):
        _ = compiled_dag.execute(1)

    # test same with asyncio
    async def main():
        a = Actor.remote(0)
        with InputNode() as inp:
            dag = a.sleep.bind(inp)
        async_compiled_dag = dag.experimental_compile(
            enable_asyncio=True, _max_inflight_executions=2
        )
        ref1 = await async_compiled_dag.execute_async(1)
        ref2 = await async_compiled_dag.execute_async(1)
        print(async_compiled_dag._execution_index)
        with pytest.raises(
            ray.exceptions.RayCgraphCapacityExceeded,
            match=(expected_error_message),
        ):
            _ = await async_compiled_dag.execute_async(1)
        _ = (ref1, ref2)

    loop = get_or_create_event_loop()
    loop.run_until_complete(main())
    # to show variables are being used and avoid destruction since
    # CompiledDagRef __del__ will release buffers and
    # increment _max_finished_execution_index
    _ = (ref1, ref2)


def test_event_profiling(ray_start_regular, monkeypatch):
    monkeypatch.setattr(ray.dag.constants, "RAY_CGRAPH_ENABLE_PROFILING", True)

    a = Actor.options(name="a").remote(0)
    b = Actor.options(name="b").remote(0)
    with InputNode() as inp:
        x = a.inc.bind(inp)
        y = b.inc.bind(inp)
        z = b.inc.bind(y)
        dag = MultiOutputNode([x, z])
    cdag = dag.experimental_compile()
    ray.get(cdag.execute(1))

    a_events = ray.get(a.get_events.remote())
    b_events = ray.get(b.get_events.remote())

    # a: 1 x READ, 1 x COMPUTE, 1 x WRITE
    assert len(a_events) == 3
    # a: 2 x READ, 2 x COMPUTE, 2 x WRITE
    assert len(b_events) == 6

    for event in a_events + b_events:
        assert event.actor_classname == "Actor"
        assert event.actor_name in ["a", "b"]
        assert event.method_name == "inc"
        assert event.operation in ["READ", "COMPUTE", "WRITE"]


@ray.remote
class TestWorker:
    def add_one(self, value):
        return value + 1

    def add(self, val1, val2):
        return val1 + val2

    def generate_torch_tensor(self, size) -> torch.Tensor:
        return torch.zeros(size)

    def add_value_to_tensor(self, value: int, tensor: torch.Tensor) -> torch.Tensor:
        """
        Add `value` to all elements of the tensor.
        """
        return tensor + value


"""
Compiled Graphs support the following two cases for the input/output of the graph:

1. Both the input and output of the graph are the driver process.
2. Both the input and output of the graph are the same actor process.

This test suite covers the second case. The second case is useful when we use
Ray Serve to deploy the Compiled Graph as a backend. In this case, the Ray Serve
replica, which is an actor, needs to be the input and output of the graph.
"""


def test_shared_memory_channel_only(shutdown_only):
    """
    Replica -> Worker -> Replica

    This test uses shared memory channels for all communication between actors.
    """

    @ray.remote
    class Replica:
        def __init__(self):
            self.w = TestWorker.remote()
            with InputNode() as inp:
                dag = self.w.add_one.bind(inp)
            self.compiled_dag = dag.experimental_compile()

        def no_op(self, value):
            return ray.get(self.compiled_dag.execute(value))

    replica = Replica.remote()
    ref = replica.no_op.remote(1)
    assert ray.get(ref) == 2


def test_intra_process_channel(shutdown_only):
    """
    Replica -> Worker -> Worker -> Replica

    This test uses IntraProcessChannel between DAG nodes on the Worker actor.
    Communication between the Replica and Worker actors is done through shared
    memory channels.
    """

    @ray.remote
    class Replica:
        def __init__(self):
            self.w = TestWorker.remote()
            with InputNode() as inp:
                dag = self.w.add_one.bind(inp)
                dag = self.w.add_one.bind(dag)
            self.compiled_dag = dag.experimental_compile()

        def call(self, value):
            return ray.get(self.compiled_dag.execute(value))

    replica = Replica.remote()
    ref = replica.call.remote(1)
    assert ray.get(ref) == 3


def test_driver_as_actor_and_actor_reading(ray_start_cluster):
    @ray.remote
    class Replica:
        def __init__(self):
            self.w = TestWorker.remote()
            self.w2 = TestWorker.remote()
            with InputNode() as inp:
                x = self.w.add_one.bind(inp)
                y = self.w2.add_one.bind(x)
                dag = MultiOutputNode([x, y])
            self.compiled_dag = dag.experimental_compile()

        def exec_and_get(self, value):
            return ray.get(self.compiled_dag.execute(value))

    replica = Replica.remote()
    result = replica.exec_and_get.remote(1)
    assert ray.get(result) == [2, 3]


def test_driver_as_actor_and_intraprocess_read(ray_start_cluster):
    @ray.remote
    class Replica:
        def __init__(self):
            self.w = TestWorker.remote()
            with InputNode() as inp:
                x = self.w.add_one.bind(inp)
                y = self.w.add_one.bind(x)
                dag = MultiOutputNode([x, y])
            self.compiled_dag = dag.experimental_compile()

        def exec_and_get(self, value):
            return ray.get(self.compiled_dag.execute(value))

    replica = Replica.remote()
    result = replica.exec_and_get.remote(1)
    assert ray.get(result) == [2, 3]


@pytest.mark.parametrize("single_fetch", [True, False])
def test_multiple_readers_multiple_writers(shutdown_only, single_fetch):
    """
    Replica -> Worker1 -> Replica
            |          |
            -> Worker2 -

    All communication in this DAG will be done through shared memory channels.
    """

    @ray.remote
    class Replica:
        def __init__(self):
            w1 = TestWorker.remote()
            w2 = TestWorker.remote()
            with InputNode() as inp:
                dag = MultiOutputNode([w1.add_one.bind(inp), w2.add_one.bind(inp)])
            self.compiled_dag = dag.experimental_compile()

        def call(self, value):
            if single_fetch:
                return [ray.get(ref) for ref in self.compiled_dag.execute(value)]
            else:
                return ray.get(self.compiled_dag.execute(value))

    replica = Replica.remote()
    ref = replica.call.remote(1)
    assert ray.get(ref) == [2, 2]


def test_multiple_readers_single_writer(shutdown_only):
    """
    Replica -> Worker1 -> Worker1 -> Replica
            |          |
            -> Worker2 -

    Communication between DAG nodes on Worker1 is done through IntraProcessChannel.
    Communication between different actors is done through shared memory channels.
    """

    @ray.remote
    class Replica:
        def __init__(self):
            w1 = TestWorker.remote()
            w2 = TestWorker.remote()
            with InputNode() as inp:
                branch1 = w1.add_one.bind(inp)
                branch2 = w2.add_one.bind(inp)
                dag = w1.add.bind(branch1, branch2)
            self.compiled_dag = dag.experimental_compile()

        def call(self, value):
            return ray.get(self.compiled_dag.execute(value))

    replica = Replica.remote()
    ref = replica.call.remote(1)
    assert ray.get(ref) == 4


@pytest.mark.parametrize("single_fetch", [True, False])
def test_single_reader_multiple_writers(shutdown_only, single_fetch):
    """
    Replica -> Worker1 -> Worker1 -> Replica
                        |          |
                        -> Worker2 -

    Communication between DAG nodes on Worker1 is done through IntraProcessChannel.
    Communication between different actors is done through shared memory channels.
    """

    @ray.remote
    class Replica:
        def __init__(self):
            w1 = TestWorker.remote()
            w2 = TestWorker.remote()
            with InputNode() as inp:
                dag = w1.add_one.bind(inp)
                dag = MultiOutputNode([w1.add_one.bind(dag), w2.add_one.bind(dag)])
            self.compiled_dag = dag.experimental_compile()

        def call(self, value):
            if single_fetch:
                return [ray.get(ref) for ref in self.compiled_dag.execute(value)]
            else:
                return ray.get(self.compiled_dag.execute(value))

    replica = Replica.remote()
    ref = replica.call.remote(1)
    assert ray.get(ref) == [3, 3]


def test_torch_tensor_type(shutdown_only):
    """
    This test simulates the pattern of deploying a stable diffusion model with
    Ray Serve. The base model takes a prompt and generates an image, which is a
    tensor. Then, the refiner model takes the image tensor and the prompt to refine
    the image. This test doesn't use the actual model but simulates the data flow.
    """

    @ray.remote
    class Replica:
        def __init__(self):
            self._base = TestWorker.remote()
            self._refiner = TestWorker.remote()

            with ray.dag.InputNode() as inp:
                dag = self._refiner.add_value_to_tensor.bind(
                    inp,
                    self._base.generate_torch_tensor.bind(
                        inp,
                    ).with_tensor_transport(),
                )
            self._cdag = dag.experimental_compile()

        def call(self, value):
            return ray.get(self._cdag.execute(value))

    replica = Replica.remote()
    ref = replica.call.remote(5)
    assert torch.equal(ray.get(ref), torch.tensor([5, 5, 5, 5, 5]))


def test_async_shutdown(shutdown_only):
    """Verify that when async API is used, shutdown doesn't hang
    because of threads joining at exit.
    """

    script = """
import asyncio
import ray
from ray.dag import InputNode, MultiOutputNode

async def main():
    @ray.remote
    class A:
        def f(self, i):
            return i

    a = A.remote()
    b = A.remote()

    with InputNode() as inp:
        x = a.f.bind(inp)
        y = b.f.bind(inp)
        dag =  MultiOutputNode([x, y])

    cdag = dag.experimental_compile(enable_asyncio=True)
    refs = await cdag.execute_async(1)
    outputs = []
    for ref in refs:
        outputs.append(await ref)
    print(outputs)

asyncio.run(main())
    """

    print(run_string_as_driver(script))


def test_multi_arg_exception(shutdown_only):
    a = Actor.remote(0)
    with InputNode() as i:
        o1, o2 = a.return_two_but_raise_exception.bind(i)
        dag = MultiOutputNode([o1, o2])

    compiled_dag = dag.experimental_compile()
    for _ in range(3):
        x, y = compiled_dag.execute(1)
        with pytest.raises(RuntimeError):
            ray.get(x)
        with pytest.raises(RuntimeError):
            ray.get(y)


def test_multi_arg_exception_async(shutdown_only):
    a = Actor.remote(0)
    with InputNode() as i:
        o1, o2 = a.return_two_but_raise_exception.bind(i)
        dag = MultiOutputNode([o1, o2])

    compiled_dag = dag.experimental_compile(enable_asyncio=True)

    async def main():
        for _ in range(3):
            x, y = await compiled_dag.execute_async(1)
            with pytest.raises(RuntimeError):
                await x
            with pytest.raises(RuntimeError):
                await y

    loop = get_or_create_event_loop()
    loop.run_until_complete(main())


def test_signature_mismatch(shutdown_only):
    @ray.remote
    class Worker:
        def w(self, x):
            return 1

        def f(self, x, *, y):
            pass

        def g(self, x, y, z=1):
            pass

    worker = Worker.remote()
    with pytest.raises(
        TypeError,
        match=(
            r"got an unexpected keyword argument 'y'\. The function `w` has a "
            r"signature `\(x\)`, but the given arguments to `bind` doesn't match\. "
            r".*args:.*kwargs:.*"
        ),
    ):
        with InputNode() as inp:
            _ = worker.w.bind(inp, y=inp)

    with pytest.raises(
        TypeError,
        match=(
            r"too many positional arguments\. The function `w` has a signature "
            r"`\(x\)`, but the given arguments to `bind` doesn't match\. "
            r"args:.*kwargs:.*"
        ),
    ):
        with InputNode() as inp:
            _ = worker.w.bind(inp, inp)

    with pytest.raises(
        TypeError,
        # Starting from Python 3.12, the error message includes "keyword-only."
        # Therefore, we need to match both "required keyword-only argument" and
        # "required argument."
        match=(
            r"missing a required (keyword-only )?argument: 'y'\. "
            r"The function `f` has a signature `\(x, \*, y\)`, "
            r"but the given arguments to `bind` doesn't match\. "
            r"args:.*kwargs:.*"
        ),
    ):
        with InputNode() as inp:
            _ = worker.f.bind(inp)

    with pytest.raises(
        TypeError,
        match=(
            r"missing a required argument: 'y'\. The function `g` has a signature "
            r"`\(x, y, z=1\)`, but the given arguments to `bind` doesn't match\. "
            r"args:.*kwargs:.*"
        ),
    ):
        with InputNode() as inp:
            _ = worker.g.bind(inp)


def test_missing_input_node():
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self, input):
            return input

        def add(self, a, b):
            return a + b

    actor = Actor.remote()

    with ray.dag.InputNode() as dag_input:
        input0, input1, input2 = dag_input[0], dag_input[1], dag_input[2]
        _ = actor.f.bind(input1)
        dag = actor.add.bind(input0, input2)

    with pytest.raises(
        ValueError,
        match="Compiled Graph expects input to be accessed "
        "using all of attributes 0, 1, 2, "
        "but 1 is unused. "
        "Ensure all input attributes are used and contribute "
        "to the computation of the Compiled Graph output.",
    ):
        dag.experimental_compile()


@pytest.mark.skipif(sys.platform == "win32", reason="Sigint not supported on Windows")
def test_sigint_get_dagref(ray_start_cluster):
    driver_script = """
import ray
from ray.dag import InputNode
import time

ray.init()

@ray.remote
class Actor:
    def sleep(self, x):
        while(True):
            time.sleep(x)

a = Actor.remote()
with InputNode() as inp:
    dag = a.sleep.bind(inp)
compiled_dag = dag.experimental_compile()
ref = compiled_dag.execute(1)
ray.get(ref, timeout=100)
"""
    driver_proc = run_string_as_driver_nonblocking(
        driver_script, env={"RAY_CGRAPH_teardown_timeout": "5"}
    )
    pid = driver_proc.pid
    # wait for graph execution to start
    time.sleep(5)
    proc = psutil.Process(pid)
    assert proc.status() == psutil.STATUS_RUNNING
    os.kill(pid, signal.SIGINT)  # ctrl+c
    # teardown will kill actors after 5 second timeout
    wait_for_pid_to_exit(pid, 10)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
