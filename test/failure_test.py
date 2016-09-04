import unittest
import ray
import time

import test_functions

class FailureTest(unittest.TestCase):

  def testUnknownSerialization(self):
    reload(test_functions)
    ray.init(start_ray_local=True, num_workers=1, driver_mode=ray.SILENT_MODE)

    test_functions.test_unknown_type.remote()
    time.sleep(0.2)
    task_info = ray.task_info()
    self.assertEqual(len(task_info["failed_tasks"]), 1)
    self.assertEqual(len(task_info["running_tasks"]), 0)

    ray.worker.cleanup()

class TaskStatusTest(unittest.TestCase):
  def testFailedTask(self):
    reload(test_functions)
    ray.init(start_ray_local=True, num_workers=3, driver_mode=ray.SILENT_MODE)

    test_functions.throw_exception_fct1.remote()
    test_functions.throw_exception_fct1.remote()
    for _ in range(100): # Retry if we need to wait longer.
      if len(ray.task_info()["failed_tasks"]) >= 2:
        break
      time.sleep(0.1)
    result = ray.task_info()
    self.assertEqual(len(result["failed_tasks"]), 2)
    task_ids = set()
    for task in result["failed_tasks"]:
      self.assertTrue(task.has_key("worker_address"))
      self.assertTrue(task.has_key("operationid"))
      self.assertTrue("Test function 1 intentionally failed." in task.get("error_message"))
      self.assertTrue(task["operationid"] not in task_ids)
      task_ids.add(task["operationid"])

    x = test_functions.throw_exception_fct2.remote()
    try:
      ray.get(x)
    except Exception as e:
      self.assertTrue("Test function 2 intentionally failed."in str(e))
    else:
      self.assertTrue(False) # ray.get should throw an exception

    x, y, z = test_functions.throw_exception_fct3.remote(1.0)
    for ref in [x, y, z]:
      try:
        ray.get(ref)
      except Exception as e:
        self.assertTrue("Test function 3 intentionally failed."in str(e))
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
    for _ in range(100): # Retry if we need to wait longer.
      if len(ray.task_info()["failed_remote_function_imports"]) >= 1:
        break
      time.sleep(0.1)
    self.assertTrue("There is a problem here." in ray.task_info()["failed_remote_function_imports"][0]["error_message"])

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
    for _ in range(100): # Retry if we need to wait longer.
      if len(ray.task_info()["failed_reusable_variable_imports"]) >= 1:
        break
      time.sleep(0.1)
    # Check that the error message is in the task info.
    self.assertTrue("The initializer failed." in ray.task_info()["failed_reusable_variable_imports"][0]["error_message"])

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
    for _ in range(100): # Retry if we need to wait longer.
      if len(ray.task_info()["failed_reinitialize_reusable_variables"]) >= 1:
        break
      time.sleep(0.1)
    # Check that the error message is in the task info.
    self.assertTrue("The reinitializer failed." in ray.task_info()["failed_reinitialize_reusable_variables"][0]["error_message"])

    ray.worker.cleanup()

if __name__ == "__main__":
  unittest.main(verbosity=2)
