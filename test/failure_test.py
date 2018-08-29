from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import ray
import sys
import tempfile
import time
import unittest

import ray.ray_constants as ray_constants
import pytest


def relevant_errors(error_type):
    return [info for info in ray.error_info() if info["type"] == error_type]


def wait_for_errors(error_type, num_errors, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(relevant_errors(error_type)) >= num_errors:
            return
        time.sleep(0.1)
    raise Exception("Timing out of wait.")


class TaskStatusTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testFailedTask(self):
        @ray.remote
        def throw_exception_fct1():
            raise Exception("Test function 1 intentionally failed.")

        @ray.remote
        def throw_exception_fct2():
            raise Exception("Test function 2 intentionally failed.")

        @ray.remote(num_return_vals=3)
        def throw_exception_fct3(x):
            raise Exception("Test function 3 intentionally failed.")

        ray.init(num_workers=3)

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

        @ray.remote
        def f():
            raise Exception("This function failed.")

        try:
            ray.get(f.remote())
        except Exception as e:
            assert "This function failed." in str(e)
        else:
            # ray.get should throw an exception.
            assert False

    def testFailImportingRemoteFunction(self):
        ray.init(num_workers=2)

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
        def g():
            return module.temporary_python_file()

        wait_for_errors(ray_constants.REGISTER_REMOTE_FUNCTION_PUSH_ERROR, 2)
        assert "No module named" in ray.error_info()[0]["message"]
        assert "No module named" in ray.error_info()[1]["message"]

        # Check that if we try to call the function it throws an exception and
        # does not hang.
        for _ in range(10):
            with pytest.raises(Exception):
                ray.get(g.remote())

        f.close()

        # Clean up the junk we added to sys.path.
        sys.path.pop(-1)

    def testFailedFunctionToRun(self):
        ray.init(num_workers=2)

        def f(worker):
            if ray.worker.global_worker.mode == ray.WORKER_MODE:
                raise Exception("Function to run failed.")

        ray.worker.global_worker.run_function_on_all_workers(f)
        wait_for_errors(ray_constants.FUNCTION_TO_RUN_PUSH_ERROR, 2)
        # Check that the error message is in the task info.
        error_info = ray.error_info()
        assert len(error_info) == 2
        assert "Function to run failed." in error_info[0]["message"]
        assert "Function to run failed." in error_info[1]["message"]

    def testFailImportingActor(self):
        ray.init(num_workers=2)

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
        class Foo(object):
            def __init__(self):
                self.x = module.temporary_python_file()

            def get_val(self):
                return 1

        # There should be no errors yet.
        assert len(ray.error_info()) == 0

        # Create an actor.
        foo = Foo.remote()

        # Wait for the error to arrive.
        wait_for_errors(ray_constants.REGISTER_ACTOR_PUSH_ERROR, 1)
        assert "No module named" in ray.error_info()[0]["message"]

        # Wait for the error from when the __init__ tries to run.
        wait_for_errors(ray_constants.TASK_PUSH_ERROR, 1)
        assert ("failed to be imported, and so cannot execute this method" in
                ray.error_info()[1]["message"])

        # Check that if we try to get the function it throws an exception and
        # does not hang.
        with pytest.raises(Exception):
            ray.get(foo.get_val.remote())

        # Wait for the error from when the call to get_val.
        wait_for_errors(ray_constants.TASK_PUSH_ERROR, 2)
        assert ("failed to be imported, and so cannot execute this method" in
                ray.error_info()[2]["message"])

        f.close()

        # Clean up the junk we added to sys.path.
        sys.path.pop(-1)


class ActorTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testFailedActorInit(self):
        ray.init(num_workers=0)

        error_message1 = "actor constructor failed"
        error_message2 = "actor method failed"

        @ray.remote
        class FailedActor(object):
            def __init__(self):
                raise Exception(error_message1)

            def get_val(self):
                return 1

            def fail_method(self):
                raise Exception(error_message2)

        a = FailedActor.remote()

        # Make sure that we get errors from a failed constructor.
        wait_for_errors(ray_constants.TASK_PUSH_ERROR, 1)
        assert len(ray.error_info()) == 1
        assert error_message1 in ray.error_info()[0]["message"]

        # Make sure that we get errors from a failed method.
        a.fail_method.remote()
        wait_for_errors(ray_constants.TASK_PUSH_ERROR, 2)
        assert len(ray.error_info()) == 2
        assert error_message2 in ray.error_info()[1]["message"]

    def testIncorrectMethodCalls(self):
        ray.init(num_workers=0)

        @ray.remote
        class Actor(object):
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


class WorkerDeath(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testWorkerRaisingException(self):
        ray.init(num_workers=1)

        @ray.remote
        def f():
            ray.worker.global_worker._get_next_task_from_local_scheduler = None

        # Running this task should cause the worker to raise an exception after
        # the task has successfully completed.
        f.remote()

        wait_for_errors(ray_constants.WORKER_CRASH_PUSH_ERROR, 1)
        wait_for_errors(ray_constants.WORKER_DIED_PUSH_ERROR, 1)
        assert len(ray.error_info()) == 2

    def testWorkerDying(self):
        ray.init(num_workers=0)

        # Define a remote function that will kill the worker that runs it.
        @ray.remote
        def f():
            eval("exit()")

        f.remote()

        wait_for_errors(ray_constants.WORKER_DIED_PUSH_ERROR, 1)

        error_info = ray.error_info()
        assert len(error_info) == 1
        assert "died or was killed while executing" in error_info[0]["message"]

    def testActorWorkerDying(self):
        ray.init(num_workers=0)

        @ray.remote
        class Actor(object):
            def kill(self):
                eval("exit()")

        @ray.remote
        def consume(x):
            pass

        a = Actor.remote()
        [obj], _ = ray.wait([a.kill.remote()], timeout=5000)
        with pytest.raises(Exception):
            ray.get(obj)
        with pytest.raises(Exception):
            ray.get(consume.remote(obj))
        wait_for_errors(ray_constants.WORKER_DIED_PUSH_ERROR, 1)

    def testActorWorkerDyingFutureTasks(self):
        ray.init(num_workers=0)

        @ray.remote
        class Actor(object):
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

    def testActorWorkerDyingNothingInProgress(self):
        ray.init(num_workers=0)

        @ray.remote
        class Actor(object):
            def getpid(self):
                return os.getpid()

        a = Actor.remote()
        pid = ray.get(a.getpid.remote())
        os.kill(pid, 9)
        time.sleep(0.1)
        task2 = a.getpid.remote()
        with pytest.raises(Exception):
            ray.get(task2)


@unittest.skipIf(
    os.environ.get("RAY_USE_XRAY") == "1",
    "This test does not work with xray yet.")
class PutErrorTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testPutError1(self):
        store_size = 10**6
        ray.worker._init(start_ray_local=True, object_store_memory=store_size)

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
            arg = single_dependency.remote(
                0, np.zeros(object_size, dtype=np.uint8))
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

    def testPutError2(self):
        # This is the same as the previous test, but it calls ray.put directly.
        store_size = 10**6
        ray.worker._init(start_ray_local=True, object_store_memory=store_size)

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


class ConfigurationTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testVersionMismatch(self):
        ray_version = ray.__version__
        ray.__version__ = "fake ray version"

        ray.init(num_workers=1)

        wait_for_errors(ray_constants.VERSION_MISMATCH_PUSH_ERROR, 1)

        ray.__version__ = ray_version


class WarningTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testExportLargeObjects(self):
        import ray.ray_constants as ray_constants

        ray.init(num_workers=1)

        large_object = np.zeros(2 * ray_constants.PICKLE_OBJECT_WARNING_SIZE)

        @ray.remote
        def f():
            large_object

        # Make sure that a warning is generated.
        wait_for_errors(ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR, 1)

        @ray.remote
        class Foo(object):
            def __init__(self):
                large_object

        Foo.remote()

        # Make sure that a warning is generated.
        wait_for_errors(ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
