import logging
import os
import signal
import sys
import threading
import time

import numpy as np
import pytest

import ray
import ray._private.ray_constants as ray_constants
import ray._private.utils
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.test_utils import (
    get_error_message,
    init_error_pubsub,
)
from ray.exceptions import ActorDiedError, GetTimeoutError, RayActorError, RayTaskError
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def test_unhandled_errors(ray_start_regular):
    @ray.remote
    def f():
        raise ValueError()

    @ray.remote
    class Actor:
        def f(self):
            raise ValueError()

    a = Actor.remote()
    num_exceptions = 0

    def interceptor(e):
        nonlocal num_exceptions
        num_exceptions += 1

    # Test we report unhandled exceptions.
    ray._private.worker._unhandled_error_handler = interceptor
    x1 = f.remote()
    x2 = a.f.remote()
    del x1
    del x2
    wait_for_condition(lambda: num_exceptions == 2)

    # Test we don't report handled exceptions.
    x1 = f.remote()
    x2 = a.f.remote()
    with pytest.raises(ray.exceptions.RayError) as err:  # noqa
        ray.get([x1, x2])
    del x1
    del x2
    time.sleep(1)
    assert num_exceptions == 2, num_exceptions

    # Test suppression with env var works.
    try:
        os.environ["RAY_IGNORE_UNHANDLED_ERRORS"] = "1"
        x1 = f.remote()
        del x1
        time.sleep(1)
        assert num_exceptions == 2, num_exceptions
    finally:
        del os.environ["RAY_IGNORE_UNHANDLED_ERRORS"]


def test_publish_error_to_driver(ray_start_regular, error_pubsub):
    address_info = ray_start_regular

    error_message = "Test error message"
    ray._private.utils.publish_error_to_driver(
        ray_constants.DASHBOARD_AGENT_DIED_ERROR,
        error_message,
        gcs_client=ray._raylet.GcsClient(address=address_info["gcs_address"]),
    )
    errors = get_error_message(
        error_pubsub, 1, ray_constants.DASHBOARD_AGENT_DIED_ERROR
    )
    assert errors[0]["type"] == ray_constants.DASHBOARD_AGENT_DIED_ERROR
    assert errors[0]["error_message"] == error_message


def test_get_throws_quickly_when_found_exception(ray_start_regular):
    # We use an actor instead of functions here. If we use functions, it's
    # very likely that two normal tasks are submitted before the first worker
    # is registered to Raylet. Since `maximum_startup_concurrency` is 1,
    # the worker pool will wait for the registration of the first worker
    # and skip starting new workers. The result is, the two tasks will be
    # executed sequentially, which breaks an assumption of this test case -
    # the two tasks run in parallel.
    @ray.remote
    class Actor(object):
        def bad_func1(self):
            raise Exception("Test function intentionally failed.")

        def bad_func2(self):
            os._exit(0)

        def slow_func(self, signal):
            ray.get(signal.wait.remote())

    def expect_exception(objects, exception):
        with pytest.raises(ray.exceptions.RayError) as err:
            ray.get(objects)
        assert issubclass(err.type, exception)

    signal1 = SignalActor.remote()
    actor = Actor.options(max_concurrency=2).remote()
    expect_exception(
        [actor.bad_func1.remote(), actor.slow_func.remote(signal1)],
        ray.exceptions.RayTaskError,
    )
    ray.get(signal1.send.remote())

    signal2 = SignalActor.remote()
    actor = Actor.options(max_concurrency=2).remote()
    expect_exception(
        [actor.bad_func2.remote(), actor.slow_func.remote(signal2)],
        ray.exceptions.RayActorError,
    )
    ray.get(signal2.send.remote())


def test_failed_actor_init(ray_start_regular, error_pubsub):
    error_message1 = "actor constructor failed"
    error_message2 = "actor method failed"

    @ray.remote
    class FailedActor:
        def __init__(self):
            raise Exception(error_message1)

        def fail_method(self):
            raise Exception(error_message2)

    a = FailedActor.remote()

    # Incoming methods will get the exception in creation task
    with pytest.raises(ray.exceptions.RayActorError) as e:
        ray.get(a.fail_method.remote())
    assert error_message1 in str(e.value)


def test_incorrect_method_calls(ray_start_regular):
    @ray.remote
    class Actor:
        def __init__(self, missing_variable_name):
            pass

        def get_val(self, x):
            pass

    # Make sure that we get errors if we call the constructor incorrectly.

    # Create an actor with too few arguments.
    with pytest.raises(Exception):
        a = Actor.remote()

    # Create an actor with too many arguments.
    with pytest.raises(Exception):
        a = Actor.remote(1, 2)

    # Create an actor the correct number of arguments.
    a = Actor.remote(1)

    # Call a method with too few arguments.
    with pytest.raises(Exception):
        a.get_val.remote()

    # Call a method with too many arguments.
    with pytest.raises(Exception):
        a.get_val.remote(1, 2)
    # Call a method that doesn't exist.
    with pytest.raises(AttributeError):
        a.nonexistent_method()
    with pytest.raises(AttributeError):
        a.nonexistent_method.remote()


def test_worker_raising_exception(ray_start_regular, error_pubsub):
    p = error_pubsub

    @ray.remote(max_calls=2)
    def f():
        # This is the only reasonable variable we can set here that makes the
        # execute_task function fail after the task got executed.
        worker = ray._private.worker.global_worker
        worker.function_actor_manager.increase_task_counter = None

    # Running this task should cause the worker to raise an exception after
    # the task has successfully completed.
    f.remote()
    errors = get_error_message(p, 1, ray_constants.WORKER_CRASH_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0]["type"] == ray_constants.WORKER_CRASH_PUSH_ERROR


def test_worker_dying(ray_start_regular, error_pubsub):
    p = error_pubsub
    # Define a remote function that will kill the worker that runs it.

    @ray.remote(max_retries=0)
    def f():
        eval("exit()")

    with pytest.raises(ray.exceptions.WorkerCrashedError):
        ray.get(f.remote())

    errors = get_error_message(p, 1, ray_constants.WORKER_DIED_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0]["type"] == ray_constants.WORKER_DIED_PUSH_ERROR
    assert "died or was killed while executing" in errors[0]["error_message"]


def test_actor_worker_dying(ray_start_regular, error_pubsub):
    p = error_pubsub

    @ray.remote
    class Actor:
        def kill(self):
            eval("exit()")

    @ray.remote
    def consume(x):
        pass

    a = Actor.remote()
    [obj], _ = ray.wait([a.kill.remote()], timeout=5)
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(obj)
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(consume.remote(obj))
    errors = get_error_message(p, 1, ray_constants.WORKER_DIED_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0]["type"] == ray_constants.WORKER_DIED_PUSH_ERROR


def test_actor_worker_dying_future_tasks(ray_start_regular, error_pubsub):
    p = error_pubsub

    @ray.remote(max_restarts=0)
    class Actor:
        def getpid(self):
            return os.getpid()

        def sleep(self):
            time.sleep(1)

    a = Actor.remote()
    pid = ray.get(a.getpid.remote())
    tasks1 = [a.sleep.remote() for _ in range(10)]
    os.kill(pid, 9)
    time.sleep(0.1)
    tasks2 = [a.sleep.remote() for _ in range(10)]
    for obj in tasks1 + tasks2:
        with pytest.raises(Exception):
            ray.get(obj)

    errors = get_error_message(p, 1, ray_constants.WORKER_DIED_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0]["type"] == ray_constants.WORKER_DIED_PUSH_ERROR


def test_actor_worker_dying_nothing_in_progress(ray_start_regular):
    @ray.remote(max_restarts=0)
    class Actor:
        def getpid(self):
            return os.getpid()

    a = Actor.remote()
    pid = ray.get(a.getpid.remote())
    os.kill(pid, 9)
    time.sleep(0.1)
    task2 = a.getpid.remote()
    with pytest.raises(Exception):
        ray.get(task2)


@pytest.mark.skipif(sys.platform == "win32", reason="Too flaky on windows")
def test_actor_scope_or_intentionally_killed_message(ray_start_regular, error_pubsub):
    p = error_pubsub

    @ray.remote
    class Actor:
        def __init__(self):
            # This log is added to debug a flaky test issue.
            print(os.getpid())

        def ping(self):
            pass

    a = Actor.remote()
    ray.get(a.ping.remote())
    del a

    a = Actor.remote()
    ray.get(a.ping.remote())
    with pytest.raises(ray.exceptions.ActorDiedError):
        ray.get(a.__ray_terminate__.remote())

    errors = get_error_message(p, 1, timeout=1)
    assert len(errors) == 0, "Should not have propogated an error - {}".format(errors)


def test_mixed_hanging_and_exception_should_not_hang(ray_start_regular):
    @ray.remote
    class Actor:
        def __init__(self, _id):
            self._id = _id

        def execute(self, fn) -> None:
            return fn(self._id)

    def print_and_raise_error(i):
        print(i)
        raise ValueError

    def print_and_sleep_forever(i):
        print(i)
        while True:
            time.sleep(3600)

    actors = [Actor.remote(i) for i in range(10)]
    refs = [actor.execute.remote(print_and_raise_error) for actor in actors[:2]]

    with pytest.raises(ValueError):
        ray.get(refs)

    refs.extend([actor.execute.remote(print_and_sleep_forever) for actor in actors[2:]])

    with pytest.raises(ValueError):
        ray.get(refs)


def test_mixed_hanging_and_died_actor_should_not_hang(ray_start_regular):
    @ray.remote
    class Actor:
        def __init__(self, _id):
            self._id = _id

        def execute(self, fn) -> None:
            return fn(self._id)

        def exit(self):
            ray.actor.exit_actor()

    def print_and_sleep_forever(i):
        print(i)
        while True:
            time.sleep(3600)

    actors = [Actor.remote(i) for i in range(10)]
    ray.get([actor.__ray_ready__.remote() for actor in actors])
    error_refs = [actor.exit.remote() for actor in actors[:2]]

    with pytest.raises(ActorDiedError):
        ray.get(error_refs)

    with pytest.raises(ActorDiedError):
        ray.get([actor.execute.remote(print_and_sleep_forever) for actor in actors])


def test_exception_chain(ray_start_regular):
    @ray.remote
    def bar():
        return 1 / 0

    @ray.remote
    def foo():
        return ray.get(bar.remote())

    r = foo.remote()
    try:
        ray.get(r)
    except ZeroDivisionError as ex:
        assert isinstance(ex, RayTaskError)


@pytest.mark.skip("This test does not work yet.")
@pytest.mark.parametrize("ray_start_object_store_memory", [10**6], indirect=True)
def test_put_error1(ray_start_object_store_memory, error_pubsub):
    p = error_pubsub
    num_objects = 3
    object_size = 4 * 10**5

    # Define a task with a single dependency, a numpy array, that returns
    # another array.
    @ray.remote
    def single_dependency(i, arg):
        arg = np.copy(arg)
        arg[0] = i
        return arg

    @ray.remote
    def put_arg_task():
        # Launch num_objects instances of the remote task, each dependent
        # on the one before it. The result of the first task should get
        # evicted.
        args = []
        arg = single_dependency.remote(0, np.zeros(object_size, dtype=np.uint8))
        for i in range(num_objects):
            arg = single_dependency.remote(i, arg)
            args.append(arg)

        # Get the last value to force all tasks to finish.
        value = ray.get(args[-1])
        assert value[0] == i

        # Get the first value (which should have been evicted) to force
        # reconstruction. Currently, since we're not able to reconstruct
        # `ray.put` objects that were evicted and whose originating tasks
        # are still running, this for-loop should hang and push an error to
        # the driver.
        ray.get(args[0])

    put_arg_task.remote()

    # Make sure we receive the correct error message.
    errors = get_error_message(p, 1, ray_constants.PUT_RECONSTRUCTION_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0]["type"] == ray_constants.PUT_RECONSTRUCTION_PUSH_ERROR


@pytest.mark.skip("This test does not work yet.")
@pytest.mark.parametrize("ray_start_object_store_memory", [10**6], indirect=True)
def test_put_error2(ray_start_object_store_memory):
    # This is the same as the previous test, but it calls ray.put directly.
    num_objects = 3
    object_size = 4 * 10**5

    # Define a task with a single dependency, a numpy array, that returns
    # another array.
    @ray.remote
    def single_dependency(i, arg):
        arg = np.copy(arg)
        arg[0] = i
        return arg

    @ray.remote
    def put_task():
        # Launch num_objects instances of the remote task, each dependent
        # on the one before it. The result of the first task should get
        # evicted.
        args = []
        arg = ray.put(np.zeros(object_size, dtype=np.uint8))
        for i in range(num_objects):
            arg = single_dependency.remote(i, arg)
            args.append(arg)

        # Get the last value to force all tasks to finish.
        value = ray.get(args[-1])
        assert value[0] == i

        # Get the first value (which should have been evicted) to force
        # reconstruction. Currently, since we're not able to reconstruct
        # `ray.put` objects that were evicted and whose originating tasks
        # are still running, this for-loop should hang and push an error to
        # the driver.
        ray.get(args[0])

    put_task.remote()

    # Make sure we receive the correct error message.
    # get_error_message(ray_constants.PUT_RECONSTRUCTION_PUSH_ERROR, 1)


def test_version_mismatch(ray_start_cluster):
    ray_version = ray.__version__
    try:
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=1)

        # Test the driver.
        ray.__version__ = "fake ray version"
        with pytest.raises(RuntimeError):
            ray.init(address="auto")

    finally:
        # Reset the version.
        ray.__version__ = ray_version


def test_export_large_objects(ray_start_regular, error_pubsub):
    p = error_pubsub
    import ray._private.ray_constants as ray_constants

    large_object = np.zeros(
        2 * ray_constants.FUNCTION_SIZE_WARN_THRESHOLD, dtype=np.uint8
    )

    @ray.remote
    def f():
        _ = large_object

    # Invoke the function so that the definition is exported.
    f.remote()

    # Make sure that a warning is generated.
    errors = get_error_message(p, 1, ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0]["type"] == ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR

    @ray.remote
    class Foo:
        def __init__(self):
            _ = large_object

    Foo.remote()

    # Make sure that a warning is generated.
    errors = get_error_message(p, 1, ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0]["type"] == ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR


def test_warning_many_actor_tasks_queued(shutdown_only):
    ray.init(num_cpus=1)
    p = init_error_pubsub()

    @ray.remote(num_cpus=1)
    class Foo:
        def f(self):
            time.sleep(1000)

    a = Foo.remote()
    [a.f.remote() for _ in range(20000)]
    errors = get_error_message(p, 2, ray_constants.EXCESS_QUEUEING_WARNING)
    msgs = [e["error_message"] for e in errors]
    assert "Warning: More than 5000 tasks are pending submission to actor" in msgs[0]
    assert "Warning: More than 10000 tasks are pending submission to actor" in msgs[1]


def test_no_warning_many_actor_tasks_queued_when_sequential(shutdown_only):
    ray.init(num_cpus=1)
    p = init_error_pubsub()

    @ray.remote(num_cpus=1)
    class Foo:
        def f(self):
            return 1

    a = Foo.remote()
    for _ in range(10000):
        assert ray.get(a.f.remote()) == 1
    errors = get_error_message(p, 1, ray_constants.EXCESS_QUEUEING_WARNING, timeout=1)
    assert len(errors) == 0


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "num_cpus": 0,
            "_system_config": {
                "raylet_death_check_interval_milliseconds": 10 * 1000,
                "health_check_initial_delay_ms": 0,
                "health_check_failure_threshold": 10,
                "health_check_period_ms": 100,
                "timeout_ms_task_wait_for_death_info": 100,
            },
            "include_dashboard": True,  # for list_actors API
        },
    ],
    indirect=True,
)
def test_actor_failover_with_bad_network(ray_start_cluster_head):
    # The test case is to cover the scenario that when an actor FO happens,
    # the caller receives the actor ALIVE notification and connects to the new
    # actor instance while there are still some tasks sent to the previous
    # actor instance haven't returned.
    #
    # It's not easy to reproduce this scenario, so we set
    # `raylet_death_check_interval_milliseconds` to a large value and add a
    # never-return function for the actor to keep the RPC connection alive
    # while killing the node to trigger actor failover. Later we send SIGKILL
    # to kill the previous actor process to let the task fail.
    #
    # The expected behavior is that after the actor is alive again and the
    # previous RPC connection is broken, tasks sent via the previous RPC
    # connection should fail but tasks sent via the new RPC connection should
    # succeed.

    cluster = ray_start_cluster_head
    node = cluster.add_node(num_cpus=1)

    @ray.remote(max_restarts=1)
    class Actor:
        def getpid(self):
            return os.getpid()

        def never_return(self):
            while True:
                time.sleep(1)
            return 0

    # The actor should be placed on the non-head node.
    actor = Actor.remote()
    pid = ray.get(actor.getpid.remote())

    # Submit a never-return task (task 1) to the actor. The return
    # object should be unready.
    obj1 = actor.never_return.remote()
    with pytest.raises(GetTimeoutError):
        ray.get(obj1, timeout=1)

    # Kill the non-head node and start a new one. Now GCS should trigger actor
    # FO. Since we changed the interval of worker checking death of Raylet,
    # the actor process won't quit in a short time.
    cluster.remove_node(node, allow_graceful=False)
    cluster.add_node(num_cpus=1)

    # The removed node will be marked as dead by GCS after 1 second and task 1
    # will return with failure after that.
    with pytest.raises(RayActorError):
        ray.get(obj1, timeout=2)

    # Wait for the actor to be alive again in a new worker process.
    def check_actor_restart():
        actors = ray.util.state.list_actors(
            detail=True
        )  # detail is needed for num_restarts to populate
        assert len(actors) == 1
        return actors[0].state == "ALIVE" and actors[0].num_restarts == 1

    wait_for_condition(check_actor_restart)

    # Kill the previous actor process.
    os.kill(pid, signal.SIGKILL)

    # Submit another task (task 2) to the actor.
    obj2 = actor.getpid.remote()

    # We should be able to get the return value of task 2 without any issue
    ray.get(obj2)


# Previously when threading.Lock is in the exception, it causes
# the serialization to fail. This test case is to cover that scenario.
def test_unserializable_exception(ray_start_regular, propagate_logs):
    class UnserializableException(Exception):
        def __init__(self):
            self.lock = threading.Lock()

    @ray.remote
    def func():
        raise UnserializableException

    with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
        ray.get(func.remote())

    assert isinstance(exc_info.value, ray.exceptions.RayTaskError)
    assert isinstance(exc_info.value.cause, ray.exceptions.RayError)
    assert "isn't serializable" in str(exc_info.value.cause)


def test_final_user_exception(ray_start_regular, propagate_logs, caplog):
    class MyFinalException(Exception):
        def __init_subclass__(cls, /, *args, **kwargs):
            raise TypeError("Can't subclass special typing classes")

    # This should error.
    with pytest.raises(MyFinalException):
        raise MyFinalException("MyFinalException from driver")

    @ray.remote
    def func():
        # This should also error. Problem is, the user exception is final so we can't
        # subclass it (raises exception if so). This means Ray cannot raise an exception
        # that can be caught as both `RayTaskError` and the user exception. So we
        # issue a warning and just raise it as `RayTaskError`. User needs to use
        # `e.cause` to get the user exception.
        raise MyFinalException("MyFinalException from task")

    with caplog.at_level(logging.WARNING, logger="ray.exceptions"):
        with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
            ray.get(func.remote())

    assert (
        "This exception is raised as RayTaskError only. You can use "
        "`ray_task_error.cause` to access the user exception."
    ) in caplog.text
    assert isinstance(exc_info.value, ray.exceptions.RayTaskError)
    assert isinstance(exc_info.value.cause, MyFinalException)
    assert str(exc_info.value.cause) == "MyFinalException from task"

    caplog.clear()


def test_transient_error_retry(monkeypatch, ray_start_cluster):
    with monkeypatch.context() as m:
        # This test submits 200 tasks with infinite retries and verifies that all tasks eventually succeed in the unstable network environment.
        m.setenv(
            "RAY_testing_rpc_failure",
            "CoreWorkerService.grpc_client.PushTask=100:25:25",
        )
        cluster = ray_start_cluster
        cluster.add_node(
            num_cpus=1,
            resources={"head": 1},
        )
        ray.init(address=cluster.address)

        @ray.remote(max_task_retries=-1, resources={"head": 1})
        class RetryActor:
            def echo(self, value):
                return value

        refs = []
        actor = RetryActor.remote()
        for i in range(200):
            refs.append(actor.echo.remote(i))
        assert ray.get(refs) == list(range(200))


@pytest.mark.parametrize("deterministic_failure", ["request", "response"])
def test_update_object_location_batch_failure(
    monkeypatch, ray_start_cluster, deterministic_failure
):
    with monkeypatch.context() as m:
        m.setenv(
            "RAY_testing_rpc_failure",
            "CoreWorkerService.grpc_client.UpdateObjectLocationBatch=1:"
            + ("100:0" if deterministic_failure == "request" else "0:100"),
        )
        cluster = ray_start_cluster
        head_node_id = cluster.add_node(
            num_cpus=0,
        ).node_id
        ray.init(address=cluster.address)
        worker_node_id = cluster.add_node(num_cpus=1).node_id

        @ray.remote(num_cpus=1)
        def create_large_object():
            return np.zeros(100 * 1024 * 1024, dtype=np.uint8)

        @ray.remote(num_cpus=0)
        def consume_large_object(obj):
            return sys.getsizeof(obj)

        obj_ref = create_large_object.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=worker_node_id, soft=False
            )
        ).remote()
        consume_ref = consume_large_object.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=head_node_id, soft=False
            )
        ).remote(obj_ref)
        assert ray.get(consume_ref, timeout=10) > 0


def test_raytaskerror_serialization(ray_start_regular):
    """Test that RayTaskError with dual exception instances can be properly serialized."""
    import ray.cloudpickle as pickle

    class MyException(Exception):
        def __init__(self, one, two):
            self.one = one
            self.two = two

        def __reduce__(self):
            return self.__class__, (self.one, self.two)

    original_exception = MyException("test 1", "test 2")
    ray_task_error = ray.exceptions.RayTaskError(
        function_name="test_function",
        traceback_str="test traceback",
        cause=original_exception,
    )

    dual_exception = ray_task_error.make_dual_exception_instance()
    pickled = pickle.dumps(dual_exception)
    unpickled = pickle.loads(pickled)

    assert isinstance(unpickled, ray.exceptions.RayTaskError)
    assert isinstance(unpickled, MyException)
    assert unpickled.one == "test 1"
    assert unpickled.two == "test 2"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
