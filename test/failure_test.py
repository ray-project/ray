from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import ray
import sys
import time

if sys.version_info >= (3, 0):
  from importlib import reload

import ray.test.test_functions as test_functions

def wait_for_errors(error_type, num_errors, timeout=10):
  start_time = time.time()
  while time.time() - start_time < timeout:
    error_info = ray.error_info()
    if len(error_info[error_type]) >= num_errors:
      return
    time.sleep(0.1)
  print("Timing out of wait.")

class FailureTest(unittest.TestCase):
  def testUnknownSerialization(self):
    reload(test_functions)
    ray.init(start_ray_local=True, num_workers=1, driver_mode=ray.SILENT_MODE)

    test_functions.test_unknown_type.remote()
    wait_for_errors(b"TaskError", 1)
    error_info = ray.error_info()
    self.assertEqual(len(error_info[b"TaskError"]), 1)

    ray.worker.cleanup()

class TaskSerializationTest(unittest.TestCase):
  def testReturnAndPassUnknownType(self):
    ray.init(start_ray_local=True, num_workers=1, driver_mode=ray.SILENT_MODE)

    class Foo(object):
      pass
    # Check that returning an unknown type from a remote function raises an
    # exception.
    @ray.remote
    def f():
      return Foo()
    self.assertRaises(Exception, lambda : ray.get(f.remote()))
    # Check that passing an unknown type into a remote function raises an
    # exception.
    @ray.remote
    def g(x):
      return 1
    self.assertRaises(Exception, lambda : g.remote(Foo()))

    ray.worker.cleanup()

class TaskStatusTest(unittest.TestCase):
  def testFailedTask(self):
    reload(test_functions)
    ray.init(start_ray_local=True, num_workers=3, driver_mode=ray.SILENT_MODE)

    test_functions.throw_exception_fct1.remote()
    test_functions.throw_exception_fct1.remote()
    wait_for_errors(b"TaskError", 2)
    result = ray.error_info()
    self.assertEqual(len(result[b"TaskError"]), 2)
    for task in result[b"TaskError"]:
      self.assertTrue(b"Test function 1 intentionally failed." in task.get(b"message"))

    x = test_functions.throw_exception_fct2.remote()
    try:
      ray.get(x)
    except Exception as e:
      self.assertTrue("Test function 2 intentionally failed." in str(e))
    else:
      self.assertTrue(False) # ray.get should throw an exception

    x, y, z = test_functions.throw_exception_fct3.remote(1.0)
    for ref in [x, y, z]:
      try:
        ray.get(ref)
      except Exception as e:
        self.assertTrue("Test function 3 intentionally failed." in str(e))
      else:
        self.assertTrue(False) # ray.get should throw an exception

    ray.worker.cleanup()

  def testFailImportingRemoteFunction(self):
    ray.init(start_ray_local=True, num_workers=2, driver_mode=ray.SILENT_MODE)

    # This example is somewhat contrived. It should be successfully pickled, and
    # then it should throw an exception when it is unpickled. This may depend a
    # bit on the specifics of our pickler.
    def reducer(*args):
      raise Exception("There is a problem here.")
    class Foo(object):
      def __init__(self):
        self.__name__ = "Foo_object"
        self.func_doc = ""
        self.__globals__ = {}
      def __reduce__(self):
        return reducer, ()
      def __call__(self):
        return
    ray.remote(Foo())
    wait_for_errors(b"RemoteFunctionImportError", 1)
    self.assertTrue(b"There is a problem here." in ray.error_info()[b"RemoteFunctionImportError"][0][b"message"])

    ray.worker.cleanup()

  def testFailImportingReusableVariable(self):
    ray.init(start_ray_local=True, num_workers=2, driver_mode=ray.SILENT_MODE)

    # This will throw an exception when the reusable variable is imported on the
    # workers.
    def initializer():
      if ray.worker.global_worker.mode == ray.WORKER_MODE:
        raise Exception("The initializer failed.")
      return 0
    ray.reusables.foo = ray.Reusable(initializer)
    wait_for_errors(b"ReusableVariableImportError", 1)
    # Check that the error message is in the task info.
    self.assertTrue(b"The initializer failed." in ray.error_info()[b"ReusableVariableImportError"][0][b"message"])

    ray.worker.cleanup()

  def testFailReinitializingVariable(self):
    ray.init(start_ray_local=True, num_workers=2, driver_mode=ray.SILENT_MODE)

    def initializer():
      return 0
    def reinitializer(foo):
      raise Exception("The reinitializer failed.")
    ray.reusables.foo = ray.Reusable(initializer, reinitializer)
    @ray.remote
    def use_foo():
      ray.reusables.foo
    use_foo.remote()
    wait_for_errors(b"ReusableVariableReinitializeError", 1)
    # Check that the error message is in the task info.
    self.assertTrue(b"The reinitializer failed." in ray.error_info()[b"ReusableVariableReinitializeError"][0][b"message"])

    ray.worker.cleanup()

  def testFailedFunctionToRun(self):
    ray.init(start_ray_local=True, num_workers=2, driver_mode=ray.SILENT_MODE)

    def f(worker):
      if ray.worker.global_worker.mode == ray.WORKER_MODE:
        raise Exception("Function to run failed.")
    ray.worker.global_worker.run_function_on_all_workers(f)
    wait_for_errors(b"FunctionToRunError", 2)
    # Check that the error message is in the task info.
    self.assertEqual(len(ray.error_info()[b"FunctionToRunError"]), 2)
    self.assertTrue(b"Function to run failed." in ray.error_info()[b"FunctionToRunError"][0][b"message"])
    self.assertTrue(b"Function to run failed." in ray.error_info()[b"FunctionToRunError"][1][b"message"])

    ray.worker.cleanup()

if __name__ == "__main__":
  unittest.main(verbosity=2)
