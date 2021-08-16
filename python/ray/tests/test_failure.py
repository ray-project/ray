import os
import sys
import tempfile
import time

import numpy as np
import pytest

import ray
import ray._private.utils
import ray.ray_constants as ray_constants
from ray.exceptions import RayTaskError
from ray.test_utils import (wait_for_condition, SignalActor, init_error_pubsub,
                            get_error_message)


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
    ray.worker._unhandled_error_handler = interceptor
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


def test_push_error_to_driver_through_redis(ray_start_regular, error_pubsub):
    address_info = ray_start_regular
    address = address_info["redis_address"]
    redis_client = ray._private.services.create_redis_client(
        address, password=ray.ray_constants.REDIS_DEFAULT_PASSWORD)
    error_message = "Test error message"
    ray._private.utils.push_error_to_driver_through_redis(
        redis_client, ray_constants.DASHBOARD_AGENT_DIED_ERROR, error_message)
    errors = get_error_message(error_pubsub, 1,
                               ray_constants.DASHBOARD_AGENT_DIED_ERROR)
    assert errors[0].type == ray_constants.DASHBOARD_AGENT_DIED_ERROR
    assert errors[0].error_message == error_message


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
        assert err.type is exception

    signal1 = SignalActor.remote()
    actor = Actor.options(max_concurrency=2).remote()
    expect_exception(
        [actor.bad_func1.remote(),
         actor.slow_func.remote(signal1)], ray.exceptions.RayTaskError)
    ray.get(signal1.send.remote())

    signal2 = SignalActor.remote()
    actor = Actor.options(max_concurrency=2).remote()
    expect_exception(
        [actor.bad_func2.remote(),
         actor.slow_func.remote(signal2)], ray.exceptions.RayActorError)
    ray.get(signal2.send.remote())


def test_failed_function_to_run(ray_start_2_cpus, error_pubsub):
    p = error_pubsub

    def f(worker):
        if ray.worker.global_worker.mode == ray.WORKER_MODE:
            raise Exception("Function to run failed.")

    ray.worker.global_worker.run_function_on_all_workers(f)
    # Check that the error message is in the task info.
    errors = get_error_message(p, 2, ray_constants.FUNCTION_TO_RUN_PUSH_ERROR)
    assert len(errors) == 2
    assert errors[0].type == ray_constants.FUNCTION_TO_RUN_PUSH_ERROR
    assert "Function to run failed." in errors[0].error_message
    assert "Function to run failed." in errors[1].error_message


def test_fail_importing_actor(ray_start_regular, error_pubsub):
    p = error_pubsub
    # Create the contents of a temporary Python file.
    temporary_python_file = """
def temporary_helper_function():
    return 1
"""

    f = tempfile.NamedTemporaryFile(suffix=".py")
    f.write(temporary_python_file.encode("ascii"))
    f.flush()
    directory = os.path.dirname(f.name)
    # Get the module name and strip ".py" from the end.
    module_name = os.path.basename(f.name)[:-3]
    sys.path.append(directory)
    module = __import__(module_name)

    # Define an actor that closes over this temporary module. This should
    # fail when it is unpickled.
    @ray.remote
    class Foo:
        def __init__(self, arg1, arg2=3):
            self.x = module.temporary_python_file()

        def get_val(self, arg1, arg2=3):
            return 1

    # There should be no errors yet.
    errors = get_error_message(p, 2)
    assert len(errors) == 0
    # Create an actor.
    foo = Foo.remote(3, arg2=0)

    errors = get_error_message(p, 2)
    assert len(errors) == 2

    for error in errors:
        # Wait for the error to arrive.
        if error.type == ray_constants.REGISTER_ACTOR_PUSH_ERROR:
            assert "No module named" in error.error_message
        else:
            # Wait for the error from when the __init__ tries to run.
            assert ("failed to be imported, and so cannot execute this method"
                    in error.error_message)

    # Check that if we try to get the function it throws an exception and
    # does not hang.
    with pytest.raises(Exception, match="failed to be imported"):
        ray.get(foo.get_val.remote(1, arg2=2))

    f.close()

    # Clean up the junk we added to sys.path.
    sys.path.pop(-1)


def test_failed_actor_init(ray_start_regular, error_pubsub):
    p = error_pubsub
    error_message1 = "actor constructor failed"
    error_message2 = "actor method failed"

    @ray.remote
    class FailedActor:
        def __init__(self):
            raise Exception(error_message1)

        def fail_method(self):
            raise Exception(error_message2)

    a = FailedActor.remote()

    # Make sure that we get errors from a failed constructor.
    errors = get_error_message(p, 1, ray_constants.TASK_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.TASK_PUSH_ERROR
    assert error_message1 in errors[0].error_message

    # Incoming methods will get the exception in creation task
    with pytest.raises(ray.exceptions.RayActorError) as e:
        ray.get(a.fail_method.remote())
    assert error_message1 in str(e.value)


def test_failed_actor_method(ray_start_regular, error_pubsub):
    p = error_pubsub
    error_message2 = "actor method failed"

    @ray.remote
    class FailedActor:
        def __init__(self):
            pass

        def fail_method(self):
            raise Exception(error_message2)

    a = FailedActor.remote()

    # Make sure that we get errors from a failed method.
    a.fail_method.remote()
    errors = get_error_message(p, 1, ray_constants.TASK_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.TASK_PUSH_ERROR
    assert error_message2 in errors[0].error_message


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
        worker = ray.worker.global_worker
        worker.function_actor_manager.increase_task_counter = None

    # Running this task should cause the worker to raise an exception after
    # the task has successfully completed.
    f.remote()
    errors = get_error_message(p, 1, ray_constants.WORKER_CRASH_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.WORKER_CRASH_PUSH_ERROR


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
    assert errors[0].type == ray_constants.WORKER_DIED_PUSH_ERROR
    assert "died or was killed while executing" in errors[0].error_message


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
    assert errors[0].type == ray_constants.WORKER_DIED_PUSH_ERROR


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
    assert errors[0].type == ray_constants.WORKER_DIED_PUSH_ERROR


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


def test_actor_scope_or_intentionally_killed_message(ray_start_regular,
                                                     error_pubsub):
    p = error_pubsub

    @ray.remote
    class Actor:
        def __init__(self):
            # This log is added to debug a flaky test issue.
            print(os.getpid())

        def ping(self):
            pass

    a = Actor.remote()
    # Without this waiting, there seems to be race condition happening
    # in the CI. This is not a fundamental fix for that, but it at least
    # makes the test less flaky.
    ray.get(a.ping.remote())
    a = Actor.remote()
    a.__ray_terminate__.remote()
    time.sleep(1)
    errors = get_error_message(p, 1)
    assert len(errors) == 0, "Should not have propogated an error - {}".format(
        errors)


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
@pytest.mark.parametrize(
    "ray_start_object_store_memory", [10**6], indirect=True)
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
        arg = single_dependency.remote(0, np.zeros(
            object_size, dtype=np.uint8))
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
    errors = get_error_message(p, 1,
                               ray_constants.PUT_RECONSTRUCTION_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.PUT_RECONSTRUCTION_PUSH_ERROR


@pytest.mark.skip("This test does not work yet.")
@pytest.mark.parametrize(
    "ray_start_object_store_memory", [10**6], indirect=True)
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


@pytest.mark.skip("Publish happeds before we subscribe it")
def test_version_mismatch(error_pubsub, shutdown_only):
    ray_version = ray.__version__
    ray.__version__ = "fake ray version"

    ray.init(num_cpus=1)
    p = error_pubsub

    errors = get_error_message(p, 1, ray_constants.VERSION_MISMATCH_PUSH_ERROR)
    assert False, errors
    assert len(errors) == 1
    assert errors[0].type == ray_constants.VERSION_MISMATCH_PUSH_ERROR

    # Reset the version.
    ray.__version__ = ray_version


def test_export_large_objects(ray_start_regular, error_pubsub):
    p = error_pubsub
    import ray.ray_constants as ray_constants

    large_object = np.zeros(
        2 * ray_constants.FUNCTION_SIZE_WARN_THRESHOLD, dtype=np.uint8)

    @ray.remote
    def f():
        large_object

    # Invoke the function so that the definition is exported.
    f.remote()

    # Make sure that a warning is generated.
    errors = get_error_message(p, 1,
                               ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR

    @ray.remote
    class Foo:
        def __init__(self):
            large_object

    Foo.remote()

    # Make sure that a warning is generated.
    errors = get_error_message(p, 1,
                               ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR


def test_warning_all_tasks_blocked(shutdown_only):
    ray.init(
        num_cpus=1, _system_config={"debug_dump_period_milliseconds": 500})
    p = init_error_pubsub()

    @ray.remote(num_cpus=1)
    class Foo:
        def f(self):
            return 0

    @ray.remote
    def f():
        # Creating both actors is not possible.
        actors = [Foo.remote() for _ in range(3)]
        for a in actors:
            ray.get(a.f.remote())

    # Run in a task to check we handle the blocked task case correctly
    f.remote()
    errors = get_error_message(p, 1, ray_constants.RESOURCE_DEADLOCK_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.RESOURCE_DEADLOCK_ERROR


def test_warning_many_actor_tasks_queued(shutdown_only):
    ray.init(num_cpus=1)
    p = init_error_pubsub()

    @ray.remote(num_cpus=1)
    class Foo:
        def f(self):
            import time
            time.sleep(1)

    a = Foo.remote()
    [a.f.remote() for _ in range(50000)]
    errors = get_error_message(p, 4, ray_constants.EXCESS_QUEUEING_WARNING)
    msgs = [e.error_message for e in errors]
    assert ("Warning: More than 5000 tasks are pending submission to actor" in
            msgs[0])
    assert ("Warning: More than 10000 tasks are pending submission to actor" in
            msgs[1])
    assert ("Warning: More than 20000 tasks are pending submission to actor" in
            msgs[2])
    assert ("Warning: More than 40000 tasks are pending submission to actor" in
            msgs[3])


def test_warning_actor_waiting_on_actor(shutdown_only):
    ray.init(
        num_cpus=1, _system_config={"debug_dump_period_milliseconds": 500})
    p = init_error_pubsub()

    @ray.remote(num_cpus=1)
    class Actor:
        pass

    a = Actor.remote()  # noqa
    b = Actor.remote()  # noqa

    errors = get_error_message(p, 1, ray_constants.RESOURCE_DEADLOCK_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.RESOURCE_DEADLOCK_ERROR


def test_warning_task_waiting_on_actor(shutdown_only):
    ray.init(
        num_cpus=1, _system_config={"debug_dump_period_milliseconds": 500})
    p = init_error_pubsub()

    @ray.remote(num_cpus=1)
    class Actor:
        pass

    a = Actor.remote()  # noqa

    @ray.remote(num_cpus=1)
    def f():
        print("f running")
        time.sleep(999)

    ids = [f.remote()]  # noqa

    errors = get_error_message(p, 1, ray_constants.RESOURCE_DEADLOCK_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.RESOURCE_DEADLOCK_ERROR


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
