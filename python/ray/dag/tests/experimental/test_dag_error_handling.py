# coding: utf-8
import copy
import logging
import os
import pickle
import re
import sys
import time

import pytest


from ray.exceptions import ActorDiedError, RayChannelError, RayChannelTimeoutError
import ray
import ray._private
import ray.cluster_utils
from ray.dag import DAGContext, InputNode, MultiOutputNode
from ray.tests.conftest import *  # noqa
from ray._common.utils import (
    get_or_create_event_loop,
)
from ray._private.test_utils import (
    run_string_as_driver_nonblocking,
    wait_for_pid_to_exit,
    SignalActor,
)
import signal

from ray.dag.tests.experimental.actor_defs import Actor

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
    @ray.remote
    class Actor:
        def __init__(self, signal_actor):
            self.signal_actor = signal_actor

        def send(self, x):
            self.signal_actor.send.remote()
            return x

    signal_actor = SignalActor.remote()
    a = Actor.remote(signal_actor)
    with InputNode() as inp:
        dag = a.send.bind(inp)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(1)
    # Give enough time for DAG execution result to be ready
    ray.get(signal_actor.wait.remote())
    time.sleep(0.1)
    # Use timeout=0 to either get result immediately or raise an exception
    result = ray.get(ref, timeout=0)
    assert result == 1


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


def test_sigint_get_dagref(ray_start_cluster):
    driver_script = """
import ray
from ray.dag import InputNode
import time

ray.init()

@ray.remote
class Actor:
    def sleep(self, x):
        time.sleep(x)

a = Actor.remote()
with InputNode() as inp:
    dag = a.sleep.bind(inp)
compiled_dag = dag.experimental_compile()
ref = compiled_dag.execute(100)
print("executing", flush=True)
ray.get(ref)
"""
    driver_proc = run_string_as_driver_nonblocking(
        driver_script, env={"RAY_CGRAPH_teardown_timeout": "0"}
    )
    # wait for graph execution to start
    assert driver_proc.stdout.readline() == b"executing\n"
    driver_proc.send_signal(signal.SIGINT)  # ctrl+c
    # teardown will kill actors after timeout
    wait_for_pid_to_exit(driver_proc.pid, 10)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
