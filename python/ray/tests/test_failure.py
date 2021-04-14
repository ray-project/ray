import json
import logging
import os
import signal
import sys
import tempfile
import threading
import time

import numpy as np
import pytest
import redis

import ray
from ray.experimental.internal_kv import _internal_kv_get
from ray.autoscaler._private.util import DEBUG_AUTOSCALING_ERROR
import ray._private.utils
from ray.util.placement_group import placement_group
import ray.ray_constants as ray_constants
from ray.exceptions import RayTaskError
from ray.cluster_utils import Cluster
from ray.test_utils import (wait_for_condition, SignalActor, init_error_pubsub,
                            get_error_message, Semaphore)


def test_failed_task(ray_start_regular, error_pubsub):
    @ray.remote
    def throw_exception_fct1():
        raise Exception("Test function 1 intentionally failed.")

    @ray.remote
    def throw_exception_fct2():
        raise Exception("Test function 2 intentionally failed.")

    @ray.remote(num_returns=3)
    def throw_exception_fct3(x):
        raise Exception("Test function 3 intentionally failed.")

    p = error_pubsub

    throw_exception_fct1.remote()
    throw_exception_fct1.remote()

    msgs = get_error_message(p, 2, ray_constants.TASK_PUSH_ERROR)
    assert len(msgs) == 2
    for msg in msgs:
        assert "Test function 1 intentionally failed." in msg.error_message

    x = throw_exception_fct2.remote()
    try:
        ray.get(x)
    except Exception as e:
        assert "Test function 2 intentionally failed." in str(e)
    else:
        # ray.get should throw an exception.
        assert False

    x, y, z = throw_exception_fct3.remote(1.0)
    for ref in [x, y, z]:
        try:
            ray.get(ref)
        except Exception as e:
            assert "Test function 3 intentionally failed." in str(e)
        else:
            # ray.get should throw an exception.
            assert False

    class CustomException(ValueError):
        pass

    @ray.remote
    def f():
        raise CustomException("This function failed.")

    try:
        ray.get(f.remote())
    except Exception as e:
        assert "This function failed." in str(e)
        assert isinstance(e, CustomException)
        assert isinstance(e, ray.exceptions.RayTaskError)
        assert "RayTaskError(CustomException)" in repr(e)
    else:
        # ray.get should throw an exception.
        assert False


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


def test_fail_importing_remote_function(ray_start_2_cpus, error_pubsub):
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

    # Define a function that closes over this temporary module. This should
    # fail when it is unpickled.
    @ray.remote
    def g(x, y=3):
        try:
            module.temporary_python_file()
        except Exception:
            # This test is not concerned with the error from running this
            # function. Only from unpickling the remote function.
            pass

    # Invoke the function so that the definition is exported.
    g.remote(1, y=2)

    errors = get_error_message(
        p, 2, ray_constants.REGISTER_REMOTE_FUNCTION_PUSH_ERROR)
    assert errors[0].type == ray_constants.REGISTER_REMOTE_FUNCTION_PUSH_ERROR
    assert "No module named" in errors[0].error_message
    assert "No module named" in errors[1].error_message

    # Check that if we try to call the function it throws an exception and
    # does not hang.
    for _ in range(10):
        with pytest.raises(
                Exception, match="This function was not imported properly."):
            ray.get(g.remote(1, y=2))

    f.close()
    # Clean up the junk we added to sys.path.
    sys.path.pop(-1)


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

    large_object = np.zeros(2 * ray_constants.PICKLE_OBJECT_WARNING_SIZE)

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


def test_warning_for_infeasible_tasks(ray_start_regular, error_pubsub):
    p = error_pubsub
    # Check that we get warning messages for infeasible tasks.

    @ray.remote(num_gpus=1)
    def f():
        pass

    @ray.remote(resources={"Custom": 1})
    class Foo:
        pass

    # This task is infeasible.
    f.remote()
    errors = get_error_message(p, 1, ray_constants.INFEASIBLE_TASK_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.INFEASIBLE_TASK_ERROR

    # This actor placement task is infeasible.
    Foo.remote()
    errors = get_error_message(p, 1, ray_constants.INFEASIBLE_TASK_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.INFEASIBLE_TASK_ERROR

    # Placement group cannot be made, but no warnings should occur.
    pg = placement_group([{"GPU": 1}], strategy="STRICT_PACK")
    pg.ready()
    f.options(placement_group=pg).remote()

    errors = get_error_message(
        p, 1, ray_constants.INFEASIBLE_TASK_ERROR, timeout=5)
    assert len(errors) == 0, errors


def test_warning_for_infeasible_zero_cpu_actor(shutdown_only):
    # Check that we cannot place an actor on a 0 CPU machine and that we get an
    # infeasibility warning (even though the actor creation task itself
    # requires no CPUs).

    ray.init(num_cpus=0)
    p = init_error_pubsub()

    @ray.remote
    class Foo:
        pass

    # The actor creation should be infeasible.
    Foo.remote()
    errors = get_error_message(p, 1, ray_constants.INFEASIBLE_TASK_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.INFEASIBLE_TASK_ERROR
    p.close()


def test_warning_for_too_many_actors(shutdown_only):
    # Check that if we run a workload which requires too many workers to be
    # started that we will receive a warning.
    num_cpus = 2
    ray.init(num_cpus=num_cpus)

    p = init_error_pubsub()

    @ray.remote
    class Foo:
        def __init__(self):
            time.sleep(1000)

    # NOTE: We should save actor, otherwise it will be out of scope.
    actor_group1 = [Foo.remote() for _ in range(num_cpus * 3)]
    assert len(actor_group1) == num_cpus * 3
    errors = get_error_message(p, 1, ray_constants.WORKER_POOL_LARGE_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.WORKER_POOL_LARGE_ERROR

    actor_group2 = [Foo.remote() for _ in range(num_cpus)]
    assert len(actor_group2) == num_cpus
    errors = get_error_message(p, 1, ray_constants.WORKER_POOL_LARGE_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.WORKER_POOL_LARGE_ERROR
    p.close()


def test_warning_for_too_many_nested_tasks(shutdown_only):
    # Check that if we run a workload which requires too many workers to be
    # started that we will receive a warning.
    num_cpus = 2
    ray.init(num_cpus=num_cpus)
    p = init_error_pubsub()

    remote_wait = Semaphore.remote(value=0)
    nested_wait = Semaphore.remote(value=0)

    ray.get([
        remote_wait.locked.remote(),
        nested_wait.locked.remote(),
    ])

    @ray.remote
    def f():
        time.sleep(1000)
        return 1

    @ray.remote
    def h(nested_waits):
        nested_wait.release.remote()
        ray.get(nested_waits)
        ray.get(f.remote())

    @ray.remote
    def g(remote_waits, nested_waits):
        # Sleep so that the f tasks all get submitted to the scheduler after
        # the g tasks.
        remote_wait.release.remote()
        # wait until every lock is released.
        ray.get(remote_waits)
        ray.get(h.remote(nested_waits))

    num_root_tasks = num_cpus * 4
    # Lock remote task until everything is scheduled.
    remote_waits = []
    nested_waits = []
    for _ in range(num_root_tasks):
        remote_waits.append(remote_wait.acquire.remote())
        nested_waits.append(nested_wait.acquire.remote())

    [g.remote(remote_waits, nested_waits) for _ in range(num_root_tasks)]

    errors = get_error_message(p, 1, ray_constants.WORKER_POOL_LARGE_ERROR)
    assert len(errors) == 1
    assert errors[0].type == ray_constants.WORKER_POOL_LARGE_ERROR
    p.close()


def test_warning_for_many_duplicate_remote_functions_and_actors(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def create_remote_function():
        @ray.remote
        def g():
            return 1

        return ray.get(g.remote())

    for _ in range(ray_constants.DUPLICATE_REMOTE_FUNCTION_THRESHOLD - 1):
        ray.get(create_remote_function.remote())

    import io
    log_capture_string = io.StringIO()
    ch = logging.StreamHandler(log_capture_string)

    # TODO(rkn): It's terrible to have to rely on this implementation detail,
    # the fact that the warning comes from ray._private.import_thread.logger.
    # However, I didn't find a good way to capture the output for all loggers
    # simultaneously.
    ray._private.import_thread.logger.addHandler(ch)

    ray.get(create_remote_function.remote())

    start_time = time.time()
    while time.time() < start_time + 10:
        log_contents = log_capture_string.getvalue()
        if len(log_contents) > 0:
            break

    ray._private.import_thread.logger.removeHandler(ch)

    assert "remote function" in log_contents
    assert "has been exported {} times.".format(
        ray_constants.DUPLICATE_REMOTE_FUNCTION_THRESHOLD) in log_contents

    # Now test the same thing but for actors.

    @ray.remote
    def create_actor_class():
        # Require a GPU so that the actor is never actually created and we
        # don't spawn an unreasonable number of processes.
        @ray.remote(num_gpus=1)
        class Foo:
            pass

        Foo.remote()

    for _ in range(ray_constants.DUPLICATE_REMOTE_FUNCTION_THRESHOLD - 1):
        ray.get(create_actor_class.remote())

    log_capture_string = io.StringIO()
    ch = logging.StreamHandler(log_capture_string)

    # TODO(rkn): As mentioned above, it's terrible to have to rely on this
    # implementation detail.
    ray._private.import_thread.logger.addHandler(ch)

    ray.get(create_actor_class.remote())

    start_time = time.time()
    while time.time() < start_time + 10:
        log_contents = log_capture_string.getvalue()
        if len(log_contents) > 0:
            break

    ray._private.import_thread.logger.removeHandler(ch)

    assert "actor" in log_contents
    assert "has been exported {} times.".format(
        ray_constants.DUPLICATE_REMOTE_FUNCTION_THRESHOLD) in log_contents


def test_redis_module_failure(ray_start_regular):
    address_info = ray_start_regular
    address = address_info["redis_address"]
    address = address.split(":")
    assert len(address) == 2

    def run_failure_test(expecting_message, *command):
        with pytest.raises(
                Exception, match=".*{}.*".format(expecting_message)):
            client = redis.StrictRedis(
                host=address[0],
                port=int(address[1]),
                password=ray_constants.REDIS_DEFAULT_PASSWORD)
            client.execute_command(*command)

    def run_one_command(*command):
        client = redis.StrictRedis(
            host=address[0],
            port=int(address[1]),
            password=ray_constants.REDIS_DEFAULT_PASSWORD)
        client.execute_command(*command)

    run_failure_test("wrong number of arguments", "RAY.TABLE_ADD", 13)
    run_failure_test("Prefix must be in the TablePrefix range",
                     "RAY.TABLE_ADD", 100000, 1, 1, 1)
    run_failure_test("Prefix must be in the TablePrefix range",
                     "RAY.TABLE_REQUEST_NOTIFICATIONS", 100000, 1, 1, 1)
    run_failure_test("Prefix must be a valid TablePrefix integer",
                     "RAY.TABLE_ADD", b"a", 1, 1, 1)
    run_failure_test("Pubsub channel must be in the TablePubsub range",
                     "RAY.TABLE_ADD", 1, 10000, 1, 1)
    run_failure_test("Pubsub channel must be a valid integer", "RAY.TABLE_ADD",
                     1, b"a", 1, 1)
    # Change the key from 1 to 2, since the previous command should have
    # succeeded at writing the key, but not publishing it.
    run_failure_test("Index is less than 0.", "RAY.TABLE_APPEND", 1, 1, 2, 1,
                     -1)
    run_failure_test("Index is not a number.", "RAY.TABLE_APPEND", 1, 1, 2, 1,
                     b"a")
    run_one_command("RAY.TABLE_APPEND", 1, 1, 2, 1)
    # It's okay to add duplicate entries.
    run_one_command("RAY.TABLE_APPEND", 1, 1, 2, 1)
    run_one_command("RAY.TABLE_APPEND", 1, 1, 2, 1, 0)
    run_one_command("RAY.TABLE_APPEND", 1, 1, 2, 1, 1)
    run_one_command("RAY.SET_ADD", 1, 1, 3, 1)
    # It's okey to add duplicate entries.
    run_one_command("RAY.SET_ADD", 1, 1, 3, 1)
    run_one_command("RAY.SET_REMOVE", 1, 1, 3, 1)
    # It's okey to remove duplicate entries.
    run_one_command("RAY.SET_REMOVE", 1, 1, 3, 1)


# Note that this test will take at least 10 seconds because it must wait for
# the monitor to detect enough missed heartbeats.
def test_warning_for_dead_node(ray_start_cluster_2_nodes, error_pubsub):
    cluster = ray_start_cluster_2_nodes
    cluster.wait_for_nodes()
    p = error_pubsub

    node_ids = {item["NodeID"] for item in ray.nodes()}

    # Try to make sure that the monitor has received at least one heartbeat
    # from the node.
    time.sleep(0.5)

    # Kill both raylets.
    cluster.list_all_nodes()[1].kill_raylet()
    cluster.list_all_nodes()[0].kill_raylet()

    # Check that we get warning messages for both raylets.
    errors = get_error_message(p, 2, ray_constants.REMOVED_NODE_ERROR, 40)

    # Extract the client IDs from the error messages. This will need to be
    # changed if the error message changes.
    warning_node_ids = {error.error_message.split(" ")[5] for error in errors}

    assert node_ids == warning_node_ids


def test_warning_for_dead_autoscaler(ray_start_regular, error_pubsub):
    # Terminate the autoscaler process.
    from ray.worker import _global_node
    autoscaler_process = _global_node.all_processes[
        ray_constants.PROCESS_TYPE_MONITOR][0].process
    autoscaler_process.terminate()

    # Confirm that we receive an autoscaler failure error.
    errors = get_error_message(
        error_pubsub, 1, ray_constants.MONITOR_DIED_ERROR, timeout=5)
    assert len(errors) == 1

    # Confirm that the autoscaler failure error is stored.
    error = _internal_kv_get(DEBUG_AUTOSCALING_ERROR)
    assert error is not None


def test_raylet_crash_when_get(ray_start_regular):
    def sleep_to_kill_raylet():
        # Don't kill raylet before default workers get connected.
        time.sleep(2)
        ray.worker._global_node.kill_raylet()

    object_ref = ray.put(np.zeros(200 * 1024, dtype=np.uint8))
    ray.internal.free(object_ref)

    thread = threading.Thread(target=sleep_to_kill_raylet)
    thread.start()
    with pytest.raises(ray.exceptions.ObjectLostError):
        ray.get(object_ref)
    thread.join()


def test_connect_with_disconnected_node(shutdown_only):
    config = {
        "num_heartbeats_timeout": 50,
        "raylet_heartbeat_period_milliseconds": 10,
    }
    cluster = Cluster()
    cluster.add_node(num_cpus=0, _system_config=config)
    ray.init(address=cluster.address)
    p = init_error_pubsub()
    errors = get_error_message(p, 1, timeout=5)
    print(errors)
    assert len(errors) == 0
    # This node is killed by SIGKILL, ray_monitor will mark it to dead.
    dead_node = cluster.add_node(num_cpus=0)
    cluster.remove_node(dead_node, allow_graceful=False)
    errors = get_error_message(p, 1, ray_constants.REMOVED_NODE_ERROR)
    assert len(errors) == 1
    # This node is killed by SIGKILL, ray_monitor will mark it to dead.
    dead_node = cluster.add_node(num_cpus=0)
    cluster.remove_node(dead_node, allow_graceful=False)
    errors = get_error_message(p, 1, ray_constants.REMOVED_NODE_ERROR)
    assert len(errors) == 1
    # This node is killed by SIGTERM, ray_monitor will not mark it again.
    removing_node = cluster.add_node(num_cpus=0)
    cluster.remove_node(removing_node, allow_graceful=True)
    errors = get_error_message(p, 1, timeout=2)
    assert len(errors) == 0
    # There is no connection error to a dead node.
    errors = get_error_message(p, 1, timeout=2)
    assert len(errors) == 0
    p.close()


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "num_cpus": 5,
        "object_store_memory": 10**8,
    }],
    indirect=True)
def test_parallel_actor_fill_plasma_retry(ray_start_cluster_head):
    @ray.remote
    class LargeMemoryActor:
        def some_expensive_task(self):
            return np.zeros(10**8 // 2, dtype=np.uint8)

    actors = [LargeMemoryActor.remote() for _ in range(5)]
    for _ in range(10):
        pending = [a.some_expensive_task.remote() for a in actors]
        while pending:
            [done], pending = ray.wait(pending, num_returns=1)


def test_fill_object_store_exception(shutdown_only):
    ray.init(
        num_cpus=2,
        object_store_memory=10**8,
        _system_config={"automatic_object_spilling_enabled": False})

    @ray.remote
    def expensive_task():
        return np.zeros((10**8) // 10, dtype=np.uint8)

    with pytest.raises(ray.exceptions.RayTaskError) as e:
        ray.get([expensive_task.remote() for _ in range(20)])
        with pytest.raises(ray.exceptions.ObjectStoreFullError):
            raise e.as_instanceof_cause()

    @ray.remote
    class LargeMemoryActor:
        def some_expensive_task(self):
            return np.zeros(10**8 + 2, dtype=np.uint8)

        def test(self):
            return 1

    actor = LargeMemoryActor.remote()
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(actor.some_expensive_task.remote())
    # Make sure actor does not die
    ray.get(actor.test.remote())

    with pytest.raises(ray.exceptions.ObjectStoreFullError):
        ray.put(np.zeros(10**8 + 2, dtype=np.uint8))


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_nodes": 1,
        "num_cpus": 2,
    }, {
        "num_nodes": 2,
        "num_cpus": 1,
    }],
    indirect=True)
def test_eviction(ray_start_cluster):
    @ray.remote
    def large_object():
        return np.zeros(10 * 1024 * 1024)

    obj = large_object.remote()
    assert (isinstance(ray.get(obj), np.ndarray))
    # Evict the object.
    ray.internal.free([obj])
    # ray.get throws an exception.
    with pytest.raises(ray.exceptions.ObjectLostError):
        ray.get(obj)

    @ray.remote
    def dependent_task(x):
        return

    # If the object is passed by reference, the task throws an
    # exception.
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(dependent_task.remote(obj))


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_nodes": 2,
        "num_cpus": 1,
    }, {
        "num_nodes": 1,
        "num_cpus": 2,
    }],
    indirect=True)
def test_serialized_id(ray_start_cluster):
    @ray.remote
    def small_object():
        # Sleep a bit before creating the object to force a timeout
        # at the getter.
        time.sleep(1)
        return 1

    @ray.remote
    def dependent_task(x):
        return x

    @ray.remote
    def get(obj_refs, test_dependent_task):
        print("get", obj_refs)
        obj_ref = obj_refs[0]
        if test_dependent_task:
            assert ray.get(dependent_task.remote(obj_ref)) == 1
        else:
            assert ray.get(obj_ref) == 1

    obj = small_object.remote()
    ray.get(get.remote([obj], False))

    obj = small_object.remote()
    ray.get(get.remote([obj], True))

    obj = ray.put(1)
    ray.get(get.remote([obj], False))

    obj = ray.put(1)
    ray.get(get.remote([obj], True))


@pytest.mark.parametrize("use_actors,node_failure",
                         [(False, False), (False, True), (True, False),
                          (True, True)])
def test_fate_sharing(ray_start_cluster, use_actors, node_failure):
    config = {
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_period_milliseconds": 100,
    }
    cluster = Cluster()
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _system_config=config)
    ray.init(address=cluster.address)
    # Node to place the parent actor.
    node_to_kill = cluster.add_node(num_cpus=1, resources={"parent": 1})
    # Node to place the child actor.
    cluster.add_node(num_cpus=1, resources={"child": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def sleep():
        time.sleep(1000)

    @ray.remote(resources={"child": 1})
    def probe():
        return

    # TODO(swang): This test does not pass if max_restarts > 0 for the
    # raylet codepath. Add this parameter once the GCS actor service is enabled
    # by default.
    @ray.remote
    class Actor(object):
        def __init__(self):
            return

        def start_child(self, use_actors):
            if use_actors:
                child = Actor.options(resources={"child": 1}).remote()
                ray.get(child.sleep.remote())
            else:
                ray.get(sleep.options(resources={"child": 1}).remote())

        def sleep(self):
            time.sleep(1000)

        def get_pid(self):
            return os.getpid()

    # Returns whether the "child" resource is available.
    def child_resource_available():
        p = probe.remote()
        ready, _ = ray.wait([p], timeout=1)
        return len(ready) > 0

    # Test fate sharing if the parent process dies.
    def test_process_failure(use_actors):
        a = Actor.options(resources={"parent": 1}).remote()
        pid = ray.get(a.get_pid.remote())
        a.start_child.remote(use_actors=use_actors)
        # Wait for the child to be scheduled.
        wait_for_condition(lambda: not child_resource_available())
        # Kill the parent process.
        os.kill(pid, 9)
        wait_for_condition(child_resource_available)

    # Test fate sharing if the parent node dies.
    def test_node_failure(node_to_kill, use_actors):
        a = Actor.options(resources={"parent": 1}).remote()
        a.start_child.remote(use_actors=use_actors)
        # Wait for the child to be scheduled.
        wait_for_condition(lambda: not child_resource_available())
        # Kill the parent process.
        cluster.remove_node(node_to_kill, allow_graceful=False)
        node_to_kill = cluster.add_node(num_cpus=1, resources={"parent": 1})
        wait_for_condition(child_resource_available)
        return node_to_kill

    if node_failure:
        test_node_failure(node_to_kill, use_actors)
    else:
        test_process_failure(use_actors)

    ray.state.state._check_connected()
    keys = [
        key for r in ray.state.state.redis_clients
        for key in r.keys("WORKER_FAILURE*")
    ]
    if node_failure:
        assert len(keys) <= 1, len(keys)
    else:
        assert len(keys) <= 2, len(keys)


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "_system_config": {
            "ping_gcs_rpc_server_max_retries": 100
        }
    }],
    indirect=True)
def test_gcs_server_failiure_report(ray_start_regular, log_pubsub):
    p = log_pubsub
    # Get gcs server pid to send a signal.
    all_processes = ray.worker._global_node.all_processes
    gcs_server_process = all_processes["gcs_server"][0].process
    gcs_server_pid = gcs_server_process.pid

    os.kill(gcs_server_pid, signal.SIGBUS)
    msg = None
    cnt = 0
    # wait for max 30 seconds.
    while cnt < 3000 and not msg:
        msg = p.get_message()
        if msg is None:
            time.sleep(0.01)
            cnt += 1
            continue
        data = json.loads(ray._private.utils.decode(msg["data"]))
        assert data["pid"] == "gcs_server"


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "_system_config": {
            "task_retry_delay_ms": 500
        }
    }],
    indirect=True)
def test_async_actor_task_retries(ray_start_regular):
    # https://github.com/ray-project/ray/issues/11683

    signal = SignalActor.remote()

    @ray.remote
    class DyingActor:
        def __init__(self):
            print("DyingActor init called")
            self.should_exit = False

        def set_should_exit(self):
            print("DyingActor.set_should_exit called")
            self.should_exit = True

        async def get(self, x, wait=False):
            print(f"DyingActor.get called with x={x}, wait={wait}")
            if self.should_exit:
                os._exit(0)
            if wait:
                await signal.wait.remote()
            return x

    # Normal in order actor task retries should work
    dying = DyingActor.options(
        max_restarts=-1,
        max_task_retries=-1,
    ).remote()

    assert ray.get(dying.get.remote(1)) == 1
    ray.get(dying.set_should_exit.remote())
    assert ray.get(dying.get.remote(42)) == 42

    # Now let's try out of order retries:
    # Task seqno 0 will return
    # Task seqno 1 will be pending and retried later
    # Task seqno 2 will return
    # Task seqno 3 will crash the actor and retried later
    dying = DyingActor.options(
        max_restarts=-1,
        max_task_retries=-1,
    ).remote()

    # seqno 0
    ref_0 = dying.get.remote(0)
    assert ray.get(ref_0) == 0
    # seqno 1
    ref_1 = dying.get.remote(1, wait=True)
    # seqno 2
    ref_2 = dying.set_should_exit.remote()
    assert ray.get(ref_2) is None
    # seqno 3, this will crash the actor because previous task set should exit
    # to true.
    ref_3 = dying.get.remote(3)

    # At this point the actor should be restarted. The two pending tasks
    # [ref_1, ref_3] should be retried, but not the completed tasks [ref_0,
    # ref_2]. Critically, if ref_2 was retried, ref_3 can never return.
    ray.get(signal.send.remote())
    assert ray.get(ref_1) == 1
    assert ray.get(ref_3) == 3


def test_raylet_node_manager_server_failure(ray_start_cluster_head,
                                            log_pubsub):
    cluster = ray_start_cluster_head
    redis_port = int(cluster.address.split(":")[1])
    # Reuse redis port to make node manager grpc server fail to start.
    cluster.add_node(wait=False, node_manager_port=redis_port)
    p = log_pubsub
    cnt = 0
    # wait for max 10 seconds.
    found = False
    while cnt < 1000 and not found:
        msg = p.get_message()
        if msg is None:
            time.sleep(0.01)
            cnt += 1
            continue
        data = json.loads(ray._private.utils.decode(msg["data"]))
        if data["pid"] == "raylet":
            found = any("Failed to start the grpc server." in line
                        for line in data["lines"])
    assert found


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
