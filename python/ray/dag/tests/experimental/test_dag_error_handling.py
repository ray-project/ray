# coding: utf-8
import copy
import logging
import pickle
import re
import signal
import sys
import time

import pytest

import ray
import ray._private
import ray.cluster_utils
from ray._common.test_utils import SignalActor
from ray._common.utils import (
    get_or_create_event_loop,
)
from ray._private.test_utils import (
    run_string_as_driver_nonblocking,
    wait_for_pid_to_exit,
)
from ray.dag import DAGContext, InputNode, MultiOutputNode
from ray.dag.tests.experimental.actor_defs import Actor
from ray.exceptions import ActorDiedError, RayChannelError, RayChannelTimeoutError
from ray.tests.conftest import *  # noqa

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


@pytest.fixture
def zero_teardown_timeout(request):
    ctx = DAGContext.get_current()
    original = ctx.teardown_timeout
    ctx.teardown_timeout = 0
    yield ctx.teardown_timeout
    ctx.teardown_timeout = original


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


def test_dag_exception_multi_output(ray_start_regular, capsys):
    # Test application throwing exceptions with a DAG with multiple outputs.
    a = Actor.remote(0)
    b = Actor.remote(0)
    with InputNode() as inp:
        dag = MultiOutputNode([a.inc.bind(inp), b.inc.bind(inp)])

    compiled_dag = dag.experimental_compile()

    # Verify that fetching each output individually raises the error.
    refs = compiled_dag.execute("hello")
    for ref in refs:
        with pytest.raises(TypeError) as exc_info:
            ray.get(ref)
        # Traceback should match the original actor class definition.
        assert "self.i += x" in str(exc_info.value)

    # Verify that another bad input exhibits the same behavior.
    refs = compiled_dag.execute("hello")
    for ref in refs:
        with pytest.raises(TypeError) as exc_info:
            ray.get(ref)
        # Traceback should match the original actor class definition.
        assert "self.i += x" in str(exc_info.value)

    # Verify that the DAG can be used after the errors.
    assert ray.get(compiled_dag.execute(1)) == [1, 1]


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


def test_get_timeout(ray_start_regular, zero_teardown_timeout):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.sleep.bind(inp)

    compiled_dag = dag.experimental_compile()
    ref = compiled_dag.execute(5)

    timed_out = False
    epsilon = 0.1  # Allow for some slack in the timeout checking
    try:
        start_time = time.monotonic()
        ray.get(ref, timeout=1)
    except RayChannelTimeoutError:
        duration = time.monotonic() - start_time
        assert duration > 1 - epsilon
        assert duration < 1 + epsilon
        timed_out = True
    assert timed_out

    compiled_dag.teardown(kill_actors=True)


def test_buffered_get_timeout(ray_start_regular, zero_teardown_timeout):
    a = Actor.remote(0)
    with InputNode() as inp:
        dag = a.sleep.bind(inp)

    compiled_dag = dag.experimental_compile()
    # The tasks will execute in order and sleep 1s, 1s, then 0s, respectively.
    refs = [
        compiled_dag.execute(1),
        compiled_dag.execute(1),
        compiled_dag.execute(0),
    ]

    with pytest.raises(RayChannelTimeoutError):
        # The final task takes <1s on its own, but because it's queued behind the
        # other two that take 1s each, this should time out.
        ray.get(refs[-1], timeout=1)

    compiled_dag.teardown(kill_actors=True)


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
    @pytest.mark.parametrize("use_multi_output_node", [False, True])
    def test_compile_twice_fails(self, ray_start_regular, use_multi_output_node: bool):
        a = Actor.remote(0)
        with InputNode() as i:
            if use_multi_output_node:
                dag = MultiOutputNode([a.echo.bind(i)])
            else:
                dag = a.echo.bind(i)
        compiled_dag = dag.experimental_compile()

        # Trying to compile again should fail.
        expected_err = (
            "It is not allowed to call `experimental_compile` on the same DAG "
            "object multiple times no matter whether `teardown` is called or not. "
            "Please reuse the existing compiled DAG or create a new one."
        )
        with pytest.raises(
            ValueError,
            match=expected_err,
        ):
            compiled_dag = dag.experimental_compile()

        # Even if we teardown the DAG, trying to compile again should still fail.
        compiled_dag.teardown()
        with pytest.raises(
            ValueError,
            match=expected_err,
        ):
            compiled_dag = dag.experimental_compile()

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


def test_exceed_max_buffered_results_multi_output(ray_start_regular):
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
        ray.get(ref[0])


def test_dag_fault_tolerance_chain(ray_start_regular):
    actors = [
        Actor.remote(0, fail_after=10 if i == 0 else None, sys_exit=False)
        for i in range(4)
    ]
    with InputNode() as i:
        dag = i
        for a in actors:
            dag = a.echo.bind(dag)

    compiled_dag = dag.experimental_compile()

    for i in range(9):
        ref = compiled_dag.execute(i)
        results = ray.get(ref)

    with pytest.raises(RuntimeError):
        for i in range(9):
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
    for i in range(10):
        ref = compiled_dag.execute(i)
        results = ray.get(ref)
        assert results == i


def test_dag_fault_tolerance(ray_start_regular):
    actors = [
        Actor.remote(0, fail_after=10 if i == 0 else None, sys_exit=False)
        for i in range(4)
    ]
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = MultiOutputNode(out)

    compiled_dag = dag.experimental_compile()

    for i in range(9):
        refs = compiled_dag.execute(1)
        assert ray.get(refs) == [i + 1] * len(actors)

    with pytest.raises(RuntimeError):
        for i in range(9, 20):
            refs = compiled_dag.execute(1)
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
    for i in range(10):
        ray.get(compiled_dag.execute(1))


def test_dag_fault_tolerance_sys_exit(ray_start_regular):
    actors = [
        Actor.remote(0, fail_after=10 if i == 0 else None, sys_exit=True)
        for i in range(4)
    ]
    with InputNode() as i:
        out = [a.inc.bind(i) for a in actors]
        dag = MultiOutputNode(out)

    compiled_dag = dag.experimental_compile()

    for i in range(9):
        refs = compiled_dag.execute(1)
        assert ray.get(refs) == [i + 1] * len(actors)

    with pytest.raises(
        ActorDiedError, match="The actor died unexpectedly before finishing this task."
    ):
        for i in range(9):
            refs = compiled_dag.execute(1)
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
    for i in range(10):
        refs = compiled_dag.execute(1)
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
    sys.exit(pytest.main(["-sv", __file__]))
