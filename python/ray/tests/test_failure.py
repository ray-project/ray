import json
import logging
import os
import sys
import tempfile
import threading
import time

import numpy as np
import pytest
import redis

import ray
import ray.ray_constants as ray_constants
from ray.cluster_utils import Cluster
from ray.test_utils import (
    relevant_errors,
    wait_for_condition,
    wait_for_errors,
    RayTestTimeoutException,
    SignalActor,
)


def test_failed_task(ray_start_regular):
    @ray.remote
    def throw_exception_fct1():
        raise Exception("Test function 1 intentionally failed.")

    @ray.remote
    def throw_exception_fct2():
        raise Exception("Test function 2 intentionally failed.")

    @ray.remote(num_return_vals=3)
    def throw_exception_fct3(x):
        raise Exception("Test function 3 intentionally failed.")

    throw_exception_fct1.remote()
    throw_exception_fct1.remote()
    wait_for_errors(ray_constants.TASK_PUSH_ERROR, 2)
    assert len(relevant_errors(ray_constants.TASK_PUSH_ERROR)) == 2
    for task in relevant_errors(ray_constants.TASK_PUSH_ERROR):
        msg = task.get("message")
        assert "Test function 1 intentionally failed." in msg

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


def test_fail_importing_remote_function(ray_start_2_cpus):
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

    wait_for_errors(ray_constants.REGISTER_REMOTE_FUNCTION_PUSH_ERROR, 2)
    errors = relevant_errors(ray_constants.REGISTER_REMOTE_FUNCTION_PUSH_ERROR)
    assert len(errors) >= 2, errors
    assert "No module named" in errors[0]["message"]
    assert "No module named" in errors[1]["message"]

    # Check that if we try to call the function it throws an exception and
    # does not hang.
    for _ in range(10):
        with pytest.raises(
                Exception, match="This function was not imported properly."):
            ray.get(g.remote(1, y=2))

    f.close()

    # Clean up the junk we added to sys.path.
    sys.path.pop(-1)


def test_failed_function_to_run(ray_start_2_cpus):
    def f(worker):
        if ray.worker.global_worker.mode == ray.WORKER_MODE:
            raise Exception("Function to run failed.")

    ray.worker.global_worker.run_function_on_all_workers(f)
    wait_for_errors(ray_constants.FUNCTION_TO_RUN_PUSH_ERROR, 2)
    # Check that the error message is in the task info.
    errors = relevant_errors(ray_constants.FUNCTION_TO_RUN_PUSH_ERROR)
    assert len(errors) == 2
    assert "Function to run failed." in errors[0]["message"]
    assert "Function to run failed." in errors[1]["message"]


def test_fail_importing_actor(ray_start_regular):
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
    assert len(ray.errors()) == 0

    # Create an actor.
    foo = Foo.remote(3, arg2=0)

    # Wait for the error to arrive.
    wait_for_errors(ray_constants.REGISTER_ACTOR_PUSH_ERROR, 1)
    errors = relevant_errors(ray_constants.REGISTER_ACTOR_PUSH_ERROR)
    assert "No module named" in errors[0]["message"]

    # Wait for the error from when the __init__ tries to run.
    wait_for_errors(ray_constants.TASK_PUSH_ERROR, 1)
    errors = relevant_errors(ray_constants.TASK_PUSH_ERROR)
    assert ("failed to be imported, and so cannot execute this method" in
            errors[0]["message"])

    # Check that if we try to get the function it throws an exception and
    # does not hang.
    with pytest.raises(Exception, match="failed to be imported"):
        ray.get(foo.get_val.remote(1, arg2=2))

    # Wait for the error from when the call to get_val.
    wait_for_errors(ray_constants.TASK_PUSH_ERROR, 2)
    errors = relevant_errors(ray_constants.TASK_PUSH_ERROR)
    assert ("failed to be imported, and so cannot execute this method" in
            errors[1]["message"])

    f.close()

    # Clean up the junk we added to sys.path.
    sys.path.pop(-1)


def test_failed_actor_init(ray_start_regular):
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
    wait_for_errors(ray_constants.TASK_PUSH_ERROR, 1)
    errors = relevant_errors(ray_constants.TASK_PUSH_ERROR)
    assert len(errors) == 1
    assert error_message1 in errors[0]["message"]

    # Make sure that we get errors from a failed method.
    a.fail_method.remote()
    wait_for_errors(ray_constants.TASK_PUSH_ERROR, 2)
    errors = relevant_errors(ray_constants.TASK_PUSH_ERROR)
    assert len(errors) == 2
    assert error_message1 in errors[1]["message"]


def test_failed_actor_method(ray_start_regular):
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
    wait_for_errors(ray_constants.TASK_PUSH_ERROR, 1)
    errors = relevant_errors(ray_constants.TASK_PUSH_ERROR)
    assert len(errors) == 1
    assert error_message2 in errors[0]["message"]


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


def test_worker_raising_exception(ray_start_regular):
    @ray.remote(max_calls=2)
    def f():
        # This is the only reasonable variable we can set here that makes the
        # execute_task function fail after the task got executed.
        worker = ray.worker.global_worker
        worker.function_actor_manager.increase_task_counter = None

    # Running this task should cause the worker to raise an exception after
    # the task has successfully completed.
    f.remote()

    wait_for_errors(ray_constants.WORKER_CRASH_PUSH_ERROR, 1)


def test_worker_dying(ray_start_regular):
    # Define a remote function that will kill the worker that runs it.
    @ray.remote(max_retries=0)
    def f():
        eval("exit()")

    with pytest.raises(ray.exceptions.RayWorkerError):
        ray.get(f.remote())

    wait_for_errors(ray_constants.WORKER_DIED_PUSH_ERROR, 1)

    errors = relevant_errors(ray_constants.WORKER_DIED_PUSH_ERROR)
    assert len(errors) == 1
    assert "died or was killed while executing" in errors[0]["message"]


def test_actor_worker_dying(ray_start_regular):
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
    wait_for_errors(ray_constants.WORKER_DIED_PUSH_ERROR, 1)


def test_actor_worker_dying_future_tasks(ray_start_regular):
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

    wait_for_errors(ray_constants.WORKER_DIED_PUSH_ERROR, 1)


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


def test_actor_scope_or_intentionally_killed_message(ray_start_regular):
    @ray.remote
    class Actor:
        pass

    a = Actor.remote()
    a = Actor.remote()
    a.__ray_terminate__.remote()
    time.sleep(1)
    assert len(
        ray.errors()) == 0, ("Should not have propogated an error - {}".format(
            ray.errors()))


@pytest.mark.skip("This test does not work yet.")
@pytest.mark.parametrize(
    "ray_start_object_store_memory", [10**6], indirect=True)
def test_put_error1(ray_start_object_store_memory):
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
    wait_for_errors(ray_constants.PUT_RECONSTRUCTION_PUSH_ERROR, 1)


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
    wait_for_errors(ray_constants.PUT_RECONSTRUCTION_PUSH_ERROR, 1)


def test_version_mismatch(shutdown_only):
    ray_version = ray.__version__
    ray.__version__ = "fake ray version"

    ray.init(num_cpus=1)

    wait_for_errors(ray_constants.VERSION_MISMATCH_PUSH_ERROR, 1)

    # Reset the version.
    ray.__version__ = ray_version


def test_export_large_objects(ray_start_regular):
    import ray.ray_constants as ray_constants

    large_object = np.zeros(2 * ray_constants.PICKLE_OBJECT_WARNING_SIZE)

    @ray.remote
    def f():
        large_object

    # Invoke the function so that the definition is exported.
    f.remote()

    # Make sure that a warning is generated.
    wait_for_errors(ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR, 1)

    @ray.remote
    class Foo:
        def __init__(self):
            large_object

    Foo.remote()

    # Make sure that a warning is generated.
    wait_for_errors(ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR, 2)


@pytest.mark.skip(reason="TODO detect resource deadlock")
def test_warning_for_resource_deadlock(shutdown_only):
    # Check that we get warning messages for infeasible tasks.
    ray.init(num_cpus=1)

    @ray.remote(num_cpus=1)
    class Foo:
        def f(self):
            return 0

    @ray.remote
    def f():
        # Creating both actors is not possible.
        actors = [Foo.remote() for _ in range(2)]
        for a in actors:
            ray.get(a.f.remote())

    # Run in a task to check we handle the blocked task case correctly
    f.remote()
    wait_for_errors(ray_constants.RESOURCE_DEADLOCK_ERROR, 1, timeout=30)


def test_warning_for_infeasible_tasks(ray_start_regular):
    # Check that we get warning messages for infeasible tasks.

    @ray.remote(num_gpus=1)
    def f():
        pass

    @ray.remote(resources={"Custom": 1})
    class Foo:
        pass

    # This task is infeasible.
    f.remote()
    wait_for_errors(ray_constants.INFEASIBLE_TASK_ERROR, 1)

    # This actor placement task is infeasible.
    Foo.remote()
    wait_for_errors(ray_constants.INFEASIBLE_TASK_ERROR, 2)


def test_warning_for_infeasible_zero_cpu_actor(shutdown_only):
    # Check that we cannot place an actor on a 0 CPU machine and that we get an
    # infeasibility warning (even though the actor creation task itself
    # requires no CPUs).

    ray.init(num_cpus=0)

    @ray.remote
    class Foo:
        pass

    # The actor creation should be infeasible.
    Foo.remote()
    wait_for_errors(ray_constants.INFEASIBLE_TASK_ERROR, 1)


def test_warning_for_too_many_actors(shutdown_only):
    # Check that if we run a workload which requires too many workers to be
    # started that we will receive a warning.
    num_cpus = 2
    ray.init(num_cpus=num_cpus)

    @ray.remote
    class Foo:
        def __init__(self):
            time.sleep(1000)

    [Foo.remote() for _ in range(num_cpus * 3)]
    wait_for_errors(ray_constants.WORKER_POOL_LARGE_ERROR, 1)
    [Foo.remote() for _ in range(num_cpus)]
    wait_for_errors(ray_constants.WORKER_POOL_LARGE_ERROR, 2)


def test_warning_for_too_many_nested_tasks(shutdown_only):
    # Check that if we run a workload which requires too many workers to be
    # started that we will receive a warning.
    num_cpus = 2
    ray.init(num_cpus=num_cpus)

    @ray.remote
    def f():
        time.sleep(1000)
        return 1

    @ray.remote
    def h():
        time.sleep(1)
        ray.get(f.remote())

    @ray.remote
    def g():
        # Sleep so that the f tasks all get submitted to the scheduler after
        # the g tasks.
        time.sleep(1)
        ray.get(h.remote())

    [g.remote() for _ in range(num_cpus * 4)]
    wait_for_errors(ray_constants.WORKER_POOL_LARGE_ERROR, 1)


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
    # the fact that the warning comes from ray.import_thread.logger. However,
    # I didn't find a good way to capture the output for all loggers
    # simultaneously.
    ray.import_thread.logger.addHandler(ch)

    ray.get(create_remote_function.remote())

    start_time = time.time()
    while time.time() < start_time + 10:
        log_contents = log_capture_string.getvalue()
        if len(log_contents) > 0:
            break

    ray.import_thread.logger.removeHandler(ch)

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
    ray.import_thread.logger.addHandler(ch)

    ray.get(create_actor_class.remote())

    start_time = time.time()
    while time.time() < start_time + 10:
        log_contents = log_capture_string.getvalue()
        if len(log_contents) > 0:
            break

    ray.import_thread.logger.removeHandler(ch)

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
def test_warning_for_dead_node(ray_start_cluster_2_nodes):
    cluster = ray_start_cluster_2_nodes
    cluster.wait_for_nodes()

    node_ids = {item["NodeID"] for item in ray.nodes()}

    # Try to make sure that the monitor has received at least one heartbeat
    # from the node.
    time.sleep(0.5)

    # Kill both raylets.
    cluster.list_all_nodes()[1].kill_raylet()
    cluster.list_all_nodes()[0].kill_raylet()

    # Check that we get warning messages for both raylets.
    wait_for_errors(ray_constants.REMOVED_NODE_ERROR, 2, timeout=40)

    # Extract the client IDs from the error messages. This will need to be
    # changed if the error message changes.
    warning_node_ids = {
        item["message"].split(" ")[5]
        for item in relevant_errors(ray_constants.REMOVED_NODE_ERROR)
    }

    assert node_ids == warning_node_ids


def test_raylet_crash_when_get(ray_start_regular):
    def sleep_to_kill_raylet():
        # Don't kill raylet before default workers get connected.
        time.sleep(2)
        ray.worker._global_node.kill_raylet()

    object_ref = ray.put(np.zeros(200 * 1024, dtype=np.uint8))
    ray.internal.free(object_ref)

    thread = threading.Thread(target=sleep_to_kill_raylet)
    thread.start()
    with pytest.raises(ray.exceptions.UnreconstructableError):
        ray.get(object_ref)
    thread.join()


def test_connect_with_disconnected_node(shutdown_only):
    config = json.dumps({
        "num_heartbeats_timeout": 50,
        "raylet_heartbeat_timeout_milliseconds": 10,
    })
    cluster = Cluster()
    cluster.add_node(num_cpus=0, _internal_config=config)
    ray.init(address=cluster.address)
    info = relevant_errors(ray_constants.REMOVED_NODE_ERROR)
    assert len(info) == 0
    # This node is killed by SIGKILL, ray_monitor will mark it to dead.
    dead_node = cluster.add_node(num_cpus=0)
    cluster.remove_node(dead_node, allow_graceful=False)
    wait_for_errors(ray_constants.REMOVED_NODE_ERROR, 1)
    # This node is killed by SIGKILL, ray_monitor will mark it to dead.
    dead_node = cluster.add_node(num_cpus=0)
    cluster.remove_node(dead_node, allow_graceful=False)
    wait_for_errors(ray_constants.REMOVED_NODE_ERROR, 2)
    # This node is killed by SIGTERM, ray_monitor will not mark it again.
    removing_node = cluster.add_node(num_cpus=0)
    cluster.remove_node(removing_node, allow_graceful=True)
    with pytest.raises(RayTestTimeoutException):
        wait_for_errors(ray_constants.REMOVED_NODE_ERROR, 3, timeout=2)
    # There is no connection error to a dead node.
    info = relevant_errors(ray_constants.RAYLET_CONNECTION_ERROR)
    assert len(info) == 0


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{
        "num_cpus": 5,
        "object_store_memory": 10**8,
        "_internal_config": json.dumps({
            "object_store_full_max_retries": 0
        })
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
        _internal_config=json.dumps({
            "object_store_full_max_retries": 0
        }))

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


def test_fill_object_store_lru_fallback(shutdown_only):
    config = json.dumps({
        "free_objects_batch_size": 1,
    })
    ray.init(
        num_cpus=2,
        object_store_memory=10**8,
        lru_evict=True,
        _internal_config=config)

    @ray.remote
    def expensive_task():
        return np.zeros((10**8) // 2, dtype=np.uint8)

    # Check that objects out of scope are cleaned up quickly.
    ray.get(expensive_task.remote())
    start = time.time()
    for _ in range(3):
        ray.get(expensive_task.remote())
    end = time.time()
    assert end - start < 3

    obj_refs = []
    for _ in range(3):
        obj_ref = expensive_task.remote()
        ray.get(obj_ref)
        obj_refs.append(obj_ref)

    @ray.remote
    class LargeMemoryActor:
        def some_expensive_task(self):
            return np.zeros(10**8 // 2, dtype=np.uint8)

        def test(self):
            return 1

    actor = LargeMemoryActor.remote()
    for _ in range(3):
        obj_ref = actor.some_expensive_task.remote()
        ray.get(obj_ref)
        obj_refs.append(obj_ref)
    # Make sure actor does not die
    ray.get(actor.test.remote())

    for _ in range(3):
        obj_ref = ray.put(np.zeros(10**8 // 2, dtype=np.uint8))
        ray.get(obj_ref)
        obj_refs.append(obj_ref)


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
    with pytest.raises(ray.exceptions.UnreconstructableError):
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
    config = json.dumps({
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_timeout_milliseconds": 100,
    })
    cluster = Cluster()
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _internal_config=config)
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


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
