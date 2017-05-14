from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import ray
import sys
import tempfile
import time
import unittest

import ray.test.test_functions as test_functions

if sys.version_info >= (3, 0):
  from importlib import reload


def relevant_errors(error_type):
  return [info for info in ray.error_info() if info[b"type"] == error_type]


def wait_for_errors(error_type, num_errors, timeout=10):
  start_time = time.time()
  while time.time() - start_time < timeout:
    if len(relevant_errors(error_type)) >= num_errors:
      return
    time.sleep(0.1)
  print("Timing out of wait.")


class FailureTest(unittest.TestCase):
  def testUnknownSerialization(self):
    reload(test_functions)
    ray.init(num_workers=1, driver_mode=ray.SILENT_MODE)

    test_functions.test_unknown_type.remote()
    wait_for_errors(b"task", 1)
    self.assertEqual(len(relevant_errors(b"task")), 1)

    ray.worker.cleanup()


class TaskSerializationTest(unittest.TestCase):
  def testReturnAndPassUnknownType(self):
    ray.init(num_workers=1, driver_mode=ray.SILENT_MODE)

    class Foo(object):
      pass

    # Check that returning an unknown type from a remote function raises an
    # exception.
    @ray.remote
    def f():
      return Foo()
    self.assertRaises(Exception, lambda: ray.get(f.remote()))

    # Check that passing an unknown type into a remote function raises an
    # exception.
    @ray.remote
    def g(x):
      return 1
    self.assertRaises(Exception, lambda: g.remote(Foo()))

    ray.worker.cleanup()


class TaskStatusTest(unittest.TestCase):
  def testFailedTask(self):
    reload(test_functions)
    ray.init(num_workers=3, driver_mode=ray.SILENT_MODE)

    test_functions.throw_exception_fct1.remote()
    test_functions.throw_exception_fct1.remote()
    wait_for_errors(b"task", 2)
    self.assertEqual(len(relevant_errors(b"task")), 2)
    for task in relevant_errors(b"task"):
      self.assertIn(b"Test function 1 intentionally failed.",
                    task.get(b"message"))

    x = test_functions.throw_exception_fct2.remote()
    try:
      ray.get(x)
    except Exception as e:
      self.assertIn("Test function 2 intentionally failed.", str(e))
    else:
      # ray.get should throw an exception.
      self.assertTrue(False)

    x, y, z = test_functions.throw_exception_fct3.remote(1.0)
    for ref in [x, y, z]:
      try:
        ray.get(ref)
      except Exception as e:
        self.assertIn("Test function 3 intentionally failed.", str(e))
      else:
        # ray.get should throw an exception.
        self.assertTrue(False)

    ray.worker.cleanup()

  def testFailImportingRemoteFunction(self):
    ray.init(num_workers=2, driver_mode=ray.SILENT_MODE)

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

    wait_for_errors(b"register_remote_function", 2)
    self.assertIn(b"No module named", ray.error_info()[0][b"message"])
    self.assertIn(b"No module named", ray.error_info()[1][b"message"])

    # Check that if we try to call the function it throws an exception and does
    # not hang.
    for _ in range(10):
      self.assertRaises(Exception, lambda: ray.get(g.remote()))

    f.close()

    # Clean up the junk we added to sys.path.
    sys.path.pop(-1)
    ray.worker.cleanup()

  def testFailImportingEnvironmentVariable(self):
    ray.init(num_workers=2, driver_mode=ray.SILENT_MODE)

    # This will throw an exception when the environment variable is imported on
    # the workers.
    def initializer():
      if ray.worker.global_worker.mode == ray.WORKER_MODE:
        raise Exception("The initializer failed.")
      return 0
    ray.env.foo = ray.EnvironmentVariable(initializer)
    wait_for_errors(b"register_environment_variable", 2)
    # Check that the error message is in the task info.
    self.assertIn(b"The initializer failed.", ray.error_info()[0][b"message"])

    ray.worker.cleanup()

  def testFailReinitializingVariable(self):
    ray.init(num_workers=2, driver_mode=ray.SILENT_MODE)

    def initializer():
      return 0

    def reinitializer(foo):
      raise Exception("The reinitializer failed.")
    ray.env.foo = ray.EnvironmentVariable(initializer, reinitializer)

    @ray.remote
    def use_foo():
      ray.env.foo
    use_foo.remote()
    wait_for_errors(b"reinitialize_environment_variable", 1)
    # Check that the error message is in the task info.
    self.assertIn(b"The reinitializer failed.",
                  ray.error_info()[0][b"message"])

    ray.worker.cleanup()

  def testFailedFunctionToRun(self):
    ray.init(num_workers=2, driver_mode=ray.SILENT_MODE)

    def f(worker):
      if ray.worker.global_worker.mode == ray.WORKER_MODE:
        raise Exception("Function to run failed.")
    ray.worker.global_worker.run_function_on_all_workers(f)
    wait_for_errors(b"function_to_run", 2)
    # Check that the error message is in the task info.
    self.assertEqual(len(ray.error_info()), 2)
    self.assertIn(b"Function to run failed.", ray.error_info()[0][b"message"])
    self.assertIn(b"Function to run failed.", ray.error_info()[1][b"message"])

    ray.worker.cleanup()

  def testFailImportingActor(self):
    ray.init(num_workers=2, driver_mode=ray.SILENT_MODE)

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

    # Define an actor that closes over this temporary module. This should fail
    # when it is unpickled.
    @ray.remote
    class Foo(object):
      def __init__(self):
        self.x = module.temporary_python_file()

      def get_val(self):
        return 1

    # There should be no errors yet.
    self.assertEqual(len(ray.error_info()), 0)

    # Create an actor.
    foo = Foo.remote()

    # Wait for the error to arrive.
    wait_for_errors(b"register_actor", 1)
    self.assertIn(b"No module named", ray.error_info()[0][b"message"])

    # Wait for the error from when the __init__ tries to run.
    wait_for_errors(b"task", 1)
    self.assertIn(b"failed to be imported, and so cannot execute this method",
                  ray.error_info()[1][b"message"])

    # Check that if we try to get the function it throws an exception and does
    # not hang.
    with self.assertRaises(Exception):
      ray.get(foo.get_val.remote())

    # Wait for the error from when the call to get_val.
    wait_for_errors(b"task", 2)
    self.assertIn(b"failed to be imported, and so cannot execute this method",
                  ray.error_info()[2][b"message"])

    f.close()

    # Clean up the junk we added to sys.path.
    sys.path.pop(-1)
    ray.worker.cleanup()


class ActorTest(unittest.TestCase):

  def testFailedActorInit(self):
    ray.init(num_workers=0, driver_mode=ray.SILENT_MODE)

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
    wait_for_errors(b"task", 1)
    self.assertEqual(len(ray.error_info()), 1)
    self.assertIn(error_message1,
                  ray.error_info()[0][b"message"].decode("ascii"))

    # Make sure that we get errors from a failed method.
    a.fail_method.remote()
    wait_for_errors(b"task", 2)
    self.assertEqual(len(ray.error_info()), 2)
    self.assertIn(error_message2,
                  ray.error_info()[1][b"message"].decode("ascii"))

    ray.worker.cleanup()

  def testIncorrectMethodCalls(self):
    ray.init(num_workers=0, driver_mode=ray.SILENT_MODE)

    @ray.remote
    class Actor(object):
      def __init__(self, missing_variable_name):
        pass

      def get_val(self, x):
        pass

    # Make sure that we get errors if we call the constructor incorrectly.

    # Create an actor with too few arguments.
    with self.assertRaises(Exception):
      a = Actor.remote()

    # Create an actor with too many arguments.
    with self.assertRaises(Exception):
      a = Actor.remote(1, 2)

    # Create an actor the correct number of arguments.
    a = Actor.remote(1)

    # Call a method with too few arguments.
    with self.assertRaises(Exception):
      a.get_val.remote()

    # Call a method with too many arguments.
    with self.assertRaises(Exception):
      a.get_val.remote(1, 2)
    # Call a method that doesn't exist.
    with self.assertRaises(AttributeError):
      a.nonexistent_method()
    with self.assertRaises(AttributeError):
      a.nonexistent_method.remote()

    ray.worker.cleanup()


class WorkerDeath(unittest.TestCase):

  def testWorkerDying(self):
    ray.init(num_workers=0, driver_mode=ray.SILENT_MODE)

    # Define a remote function that will kill the worker that runs it.
    @ray.remote
    def f():
      eval("exit()")

    f.remote()

    wait_for_errors(b"worker_died", 1)

    self.assertEqual(len(ray.error_info()), 1)
    self.assertIn("A worker died or was killed while executing a task.",
                  ray.error_info()[0][b"message"].decode("ascii"))

    ray.worker.cleanup()


if __name__ == "__main__":
  unittest.main(verbosity=2)
