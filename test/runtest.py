import unittest
import ray
import numpy as np
import time
import subprocess32 as subprocess
import os
import sys

import test_functions
import ray.array.remote as ra
import ray.array.distributed as da

RAY_TEST_OBJECTS = [[1, "hello", 3.0], 42, 43L, "hello world", 42.0, 1L << 62,
                    (1.0, "hi"), None, (None, None), ("hello", None),
                    True, False, (True, False),
                    {True: "hello", False: "world"},
                    {"hello" : "world", 1: 42, 1.0: 45}, {},
                    np.int8(3), np.int32(4), np.int64(5),
                    np.uint8(3), np.uint32(4), np.uint64(5),
                    np.float32(1.0), np.float64(1.0)]

class UserDefinedType(object):
  def __init__(self):
    pass

  def deserialize(self, primitives):
    return "user defined type"

  def serialize(self):
    return "user defined type"

class SerializationTest(unittest.TestCase):

  def roundTripTest(self, data):
    serialized, _ = ray.serialization.serialize(ray.worker.global_worker.handle, data)
    result = ray.serialization.deserialize(ray.worker.global_worker.handle, serialized)
    self.assertEqual(data, result)

  def numpyTypeTest(self, typ):
    a = np.random.randint(0, 10, size=(100, 100)).astype(typ)
    b, _ = ray.serialization.serialize(ray.worker.global_worker.handle, a)
    c = ray.serialization.deserialize(ray.worker.global_worker.handle, b)
    self.assertTrue((a == c).all())

    a = np.array(0).astype(typ)
    b, _ = ray.serialization.serialize(ray.worker.global_worker.handle, a)
    c = ray.serialization.deserialize(ray.worker.global_worker.handle, b)
    self.assertTrue((a == c).all())

    a = np.empty((0,)).astype(typ)
    b, _ = ray.serialization.serialize(ray.worker.global_worker.handle, a)
    c = ray.serialization.deserialize(ray.worker.global_worker.handle, b)
    self.assertEqual(a.dtype, c.dtype)

  def testSerialize(self):
    ray.services.start_ray_local()

    for val in RAY_TEST_OBJECTS:
      self.roundTripTest(val)

    a = np.zeros((100, 100))
    res, _ = ray.serialization.serialize(ray.worker.global_worker.handle, a)
    b = ray.serialization.deserialize(ray.worker.global_worker.handle, res)
    self.assertTrue((a == b).all())

    self.numpyTypeTest("int8")
    self.numpyTypeTest("uint8")
    self.numpyTypeTest("int16")
    self.numpyTypeTest("uint16")
    self.numpyTypeTest("int32")
    self.numpyTypeTest("uint32")
    self.numpyTypeTest("float32")
    self.numpyTypeTest("float64")

    ref0 = ray.put(0)
    ref1 = ray.put(0)
    ref2 = ray.put(0)
    ref3 = ray.put(0)

    a = np.array([[ref0, ref1], [ref2, ref3]])
    capsule, _ = ray.serialization.serialize(ray.worker.global_worker.handle, a)
    result = ray.serialization.deserialize(ray.worker.global_worker.handle, capsule)
    self.assertTrue((a == result).all())

    self.roundTripTest(ref0)
    self.roundTripTest([ref0, ref1, ref2, ref3])
    self.roundTripTest({"0": ref0, "1": ref1, "2": ref2, "3": ref3})
    self.roundTripTest((ref0, 1))

    ray.services.cleanup()

class ObjStoreTest(unittest.TestCase):

  # Test setting up object stores, transfering data between them and retrieving data to a client
  def testObjStore(self):
    [w1, w2] = ray.services.start_services_local(return_drivers=True, num_objstores=2, num_workers_per_objstore=0)

    # putting and getting an object shouldn't change it
    for data in ["h", "h" * 10000, 0, 0.0]:
      objref = ray.put(data, w1)
      result = ray.get(objref, w1)
      self.assertEqual(result, data)

    # putting an object, shipping it to another worker, and getting it shouldn't change it
    for data in ["h", "h" * 10000, 0, 0.0, [1, 2, 3, "a", (1, 2)], ("a", ("b", 3))]:
      objref = ray.put(data, w1)
      result = ray.get(objref, w2)
      self.assertEqual(result, data)

    # putting an array, shipping it to another worker, and getting it shouldn't change it
    for data in [np.zeros([10, 20]), np.random.normal(size=[45, 25])]:
      objref = ray.put(data, w1)
      result = ray.get(objref, w2)
      self.assertTrue(np.alltrue(result == data))

    # getting multiple times shouldn't matter
    # for data in [np.zeros([10, 20]), np.random.normal(size=[45, 25]), np.zeros([10, 20], dtype=np.dtype("float64")), np.zeros([10, 20], dtype=np.dtype("float32")), np.zeros([10, 20], dtype=np.dtype("int64")), np.zeros([10, 20], dtype=np.dtype("int32"))]:
    #   objref = worker.put(data, w1)
    #   result = worker.get(objref, w2)
    #   result = worker.get(objref, w2)
    #   result = worker.get(objref, w2)
    #   self.assertTrue(np.alltrue(result == data))

    # shipping a numpy array inside something else should be fine
    data = ("a", np.random.normal(size=[10, 10]))
    objref = ray.put(data, w1)
    result = ray.get(objref, w2)
    self.assertEqual(data[0], result[0])
    self.assertTrue(np.alltrue(data[1] == result[1]))

    # shipping a numpy array inside something else should be fine
    data = ["a", np.random.normal(size=[10, 10])]
    objref = ray.put(data, w1)
    result = ray.get(objref, w2)
    self.assertEqual(data[0], result[0])
    self.assertTrue(np.alltrue(data[1] == result[1]))

    # Getting a buffer after modifying it before it finishes should return updated buffer
    objref =  ray.lib.get_objref(w1.handle)
    buf = ray.libraylib.allocate_buffer(w1.handle, objref, 100)
    buf[0][0] = 1
    ray.libraylib.finish_buffer(w1.handle, objref, buf[1], 0)
    completedbuffer = ray.libraylib.get_buffer(w1.handle, objref)
    self.assertEqual(completedbuffer[0][0], 1)

    ray.services.cleanup()

class WorkerTest(unittest.TestCase):

  def testPutGet(self):
    ray.services.start_ray_local()

    for i in range(100):
      value_before = i * 10 ** 6
      objref = ray.put(value_before)
      value_after = ray.get(objref)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = i * 10 ** 6 * 1.0
      objref = ray.put(value_before)
      value_after = ray.get(objref)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = "h" * i
      objref = ray.put(value_before)
      value_after = ray.get(objref)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = [1] * i
      objref = ray.put(value_before)
      value_after = ray.get(objref)
      self.assertEqual(value_before, value_after)

    ray.services.cleanup()

class APITest(unittest.TestCase):

  def testObjRefAliasing(self):
    reload(test_functions)
    ray.services.start_ray_local(num_workers=3, driver_mode=ray.SILENT_MODE)

    ref = test_functions.test_alias_f()
    self.assertTrue(np.alltrue(ray.get(ref) == np.ones([3, 4, 5])))
    ref = test_functions.test_alias_g()
    self.assertTrue(np.alltrue(ray.get(ref) == np.ones([3, 4, 5])))
    ref = test_functions.test_alias_h()
    self.assertTrue(np.alltrue(ray.get(ref) == np.ones([3, 4, 5])))

    ray.services.cleanup()

  def testKeywordArgs(self):
    reload(test_functions)
    ray.services.start_ray_local(num_workers=1)

    x = test_functions.keyword_fct1(1)
    self.assertEqual(ray.get(x), "1 hello")
    x = test_functions.keyword_fct1(1, "hi")
    self.assertEqual(ray.get(x), "1 hi")
    x = test_functions.keyword_fct1(1, b="world")
    self.assertEqual(ray.get(x), "1 world")

    x = test_functions.keyword_fct2(a="w", b="hi")
    self.assertEqual(ray.get(x), "w hi")
    x = test_functions.keyword_fct2(b="hi", a="w")
    self.assertEqual(ray.get(x), "w hi")
    x = test_functions.keyword_fct2(a="w")
    self.assertEqual(ray.get(x), "w world")
    x = test_functions.keyword_fct2(b="hi")
    self.assertEqual(ray.get(x), "hello hi")
    x = test_functions.keyword_fct2("w")
    self.assertEqual(ray.get(x), "w world")
    x = test_functions.keyword_fct2("w", "hi")
    self.assertEqual(ray.get(x), "w hi")

    x = test_functions.keyword_fct3(0, 1, c="w", d="hi")
    self.assertEqual(ray.get(x), "0 1 w hi")
    x = test_functions.keyword_fct3(0, 1, d="hi", c="w")
    self.assertEqual(ray.get(x), "0 1 w hi")
    x = test_functions.keyword_fct3(0, 1, c="w")
    self.assertEqual(ray.get(x), "0 1 w world")
    x = test_functions.keyword_fct3(0, 1, d="hi")
    self.assertEqual(ray.get(x), "0 1 hello hi")
    x = test_functions.keyword_fct3(0, 1)
    self.assertEqual(ray.get(x), "0 1 hello world")

    ray.services.cleanup()

  def testVariableNumberOfArgs(self):
    reload(test_functions)
    ray.services.start_ray_local(num_workers=1)

    x = test_functions.varargs_fct1(0, 1, 2)
    self.assertEqual(ray.get(x), "0 1 2")
    x = test_functions.varargs_fct2(0, 1, 2)
    self.assertEqual(ray.get(x), "1 2")

    self.assertTrue(test_functions.kwargs_exception_thrown)
    self.assertTrue(test_functions.varargs_and_kwargs_exception_thrown)

    ray.services.cleanup()

  def testNoArgs(self):
    reload(test_functions)
    ray.services.start_ray_local(num_workers=1, driver_mode=ray.SILENT_MODE)

    test_functions.no_op()
    time.sleep(0.2)
    task_info = ray.task_info()
    self.assertEqual(len(task_info["failed_tasks"]), 0)
    self.assertEqual(len(task_info["running_tasks"]), 0)
    self.assertEqual(task_info["num_succeeded"], 1)

    test_functions.no_op_fail()
    time.sleep(0.2)
    task_info = ray.task_info()
    self.assertEqual(len(task_info["failed_tasks"]), 1)
    self.assertEqual(len(task_info["running_tasks"]), 0)
    self.assertEqual(task_info["num_succeeded"], 1)
    self.assertTrue("The @remote decorator for function test_functions.no_op_fail has 0 return values, but test_functions.no_op_fail returned more than 0 values." in task_info["failed_tasks"][0].get("error_message"))

    ray.services.cleanup()

  def testTypeChecking(self):
    reload(test_functions)
    ray.services.start_ray_local(num_workers=1, driver_mode=ray.SILENT_MODE)

    # Make sure that these functions throw exceptions because there return
    # values do not type check.
    test_functions.test_return1()
    test_functions.test_return2()
    time.sleep(0.2)
    task_info = ray.task_info()
    self.assertEqual(len(task_info["failed_tasks"]), 2)
    self.assertEqual(len(task_info["running_tasks"]), 0)
    self.assertEqual(task_info["num_succeeded"], 0)

    ray.services.cleanup()

  def testDefiningRemoteFunctions(self):
    ray.services.start_ray_local(num_workers=2)

    # Test that we can define a remote function in the shell.
    @ray.remote([int], [int])
    def f(x):
      return x + 1
    self.assertEqual(ray.get(f(0)), 1)

    # Test that we can redefine the remote function.
    @ray.remote([int], [int])
    def f(x):
      return x + 10
    self.assertEqual(ray.get(f(0)), 10)

    # Test that we can close over plain old data.
    data = [np.zeros([3, 5]), (1, 2, "a"), [0.0, 1.0, 2L], 2L, {"a": np.zeros(3)}]
    @ray.remote([], [list])
    def g():
      return data
    ray.get(g())

    # Test that we can close over modules.
    @ray.remote([], [np.ndarray])
    def h():
      return np.zeros([3, 5])
    self.assertTrue(np.alltrue(ray.get(h()) == np.zeros([3, 5])))
    @ray.remote([], [float])
    def j():
      return time.time()
    ray.get(j())

    # Test that we can define remote functions that call other remote functions.
    @ray.remote([int], [int])
    def k(x):
      return x + 1
    @ray.remote([int], [int])
    def l(x):
      return k(x)
    @ray.remote([int], [int])
    def m(x):
      return ray.get(l(x))
    self.assertEqual(ray.get(k(1)), 2)
    self.assertEqual(ray.get(l(1)), 2)
    self.assertEqual(ray.get(m(1)), 2)

    ray.services.cleanup()

  def testCachingReusables(self):
    # Test that we can define reusable variables before the driver is connected.
    def foo_initializer():
      return 1
    def bar_initializer():
      return []
    def bar_reinitializer(bar):
      return []
    ray.reusables.foo = ray.Reusable(foo_initializer)
    ray.reusables.bar = ray.Reusable(bar_initializer, bar_reinitializer)

    @ray.remote([], [int])
    def use_foo():
      return ray.reusables.foo
    @ray.remote([], [list])
    def use_bar():
      ray.reusables.bar.append(1)
      return ray.reusables.bar

    ray.services.start_ray_local(num_workers=2)

    self.assertEqual(ray.get(use_foo()), 1)
    self.assertEqual(ray.get(use_foo()), 1)
    self.assertEqual(ray.get(use_bar()), [1])
    self.assertEqual(ray.get(use_bar()), [1])

    ray.services.cleanup()

class TaskStatusTest(unittest.TestCase):
  def testFailedTask(self):
    reload(test_functions)
    ray.services.start_ray_local(num_workers=3, driver_mode=ray.SILENT_MODE)

    test_functions.test_alias_f()
    test_functions.throw_exception_fct1()
    test_functions.throw_exception_fct1()
    time.sleep(1)
    result = ray.task_info()
    self.assertEqual(len(result["failed_tasks"]), 2)
    task_ids = set()
    for task in result["failed_tasks"]:
      self.assertTrue(task.has_key("worker_address"))
      self.assertTrue(task.has_key("operationid"))
      self.assertTrue("Test function 1 intentionally failed." in task.get("error_message"))
      self.assertTrue(task["operationid"] not in task_ids)
      task_ids.add(task["operationid"])

    x = test_functions.throw_exception_fct2()
    try:
      ray.get(x)
    except Exception as e:
      self.assertTrue("Test function 2 intentionally failed."in str(e))
    else:
      self.assertTrue(False) # ray.get should throw an exception

    x, y, z = test_functions.throw_exception_fct3(1.0)
    for ref in [x, y, z]:
      try:
        ray.get(ref)
      except Exception as e:
        self.assertTrue("Test function 3 intentionally failed."in str(e))
      else:
        self.assertTrue(False) # ray.get should throw an exception

    ray.services.cleanup()

def check_get_deallocated(data):
  x = ray.put(data)
  ray.get(x)
  return x.val

def check_get_not_deallocated(data):
  x = ray.put(data)
  y = ray.get(x)
  return y, x.val

class ReferenceCountingTest(unittest.TestCase):

  def testDeallocation(self):
    reload(test_functions)
    for module in [ra.core, ra.random, ra.linalg, da.core, da.random, da.linalg]:
      reload(module)
    ray.services.start_ray_local(num_workers=1)

    x = test_functions.test_alias_f()
    ray.get(x)
    time.sleep(0.1)
    objref_val = x.val
    self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val], 1)

    del x
    self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val], -1) # -1 indicates deallocated

    y = test_functions.test_alias_h()
    ray.get(y)
    time.sleep(0.1)
    objref_val = y.val
    self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)], [1, 0, 0])

    del y
    self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)], [-1, -1, -1])

    z = da.zeros([da.BLOCK_SIZE, 2 * da.BLOCK_SIZE])
    time.sleep(0.1)
    objref_val = z.val
    self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)], [1, 1, 1])

    del z
    time.sleep(0.1)
    self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)], [-1, -1, -1])

    x = ra.zeros([10, 10])
    y = ra.zeros([10, 10])
    z = ra.dot(x, y)
    objref_val = x.val
    time.sleep(0.1)
    self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)], [1, 1, 1])

    del x
    time.sleep(0.1)
    self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)], [-1, 1, 1])
    del y
    time.sleep(0.1)
    self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)], [-1, -1, 1])
    del z
    time.sleep(0.1)
    self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)], [-1, -1, -1])

    ray.services.cleanup()

  def testGet(self):
    ray.services.start_ray_local(num_workers=3)

    for val in RAY_TEST_OBJECTS + [np.zeros((2, 2)), UserDefinedType()]:
      objref_val = check_get_deallocated(val)
      self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val], -1)

      if not isinstance(val, bool) and not isinstance(val, np.generic) and val is not None:
        x, objref_val = check_get_not_deallocated(val)
        self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val], 1)

    # The following currently segfaults: The second "result = " closes the
    # memory segment as soon as the assignment is done (and the first result
    # goes out of scope).
    # data = np.zeros([10, 20])
    # objref = ray.put(data)
    # result = worker.get(objref)
    # result = worker.get(objref)
    # self.assertTrue(np.alltrue(result == data))

    ray.services.cleanup()

  # @unittest.expectedFailure
  # def testGetFailing(self):
  #   worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_worker.py")
  #   ray.services.start_ray_local(num_workers=3, worker_path=worker_path)

  #   # This is failing, because for bool and None, we cannot track python
  #   # refcounts and therefore cannot keep the refcount up
  #   # (see 5281bd414f6b404f61e1fe25ec5f6651defee206).
  #   # The resulting behavior is still correct however because True, False and
  #   # None are returned by get "by value" and therefore can be reclaimed from
  #   # the object store safely.
  # for val in [True, False, None]:
  #    x, objref_val = check_get_not_deallocated(val)
  #   self.assertEqual(ray.scheduler_info()["reference_counts"][objref_val], 1)

  # ray.services.cleanup()

class PythonModeTest(unittest.TestCase):

  def testPythonMode(self):
    reload(test_functions)
    ray.services.start_ray_local(driver_mode=ray.PYTHON_MODE)

    xref = test_functions.test_alias_h()
    self.assertTrue(np.alltrue(xref == np.ones([3, 4, 5]))) # remote functions should return by value
    self.assertTrue(np.alltrue(xref == ray.get(xref))) # ray.get should be the identity
    y = np.random.normal(size=[11, 12])
    self.assertTrue(np.alltrue(y == ray.put(y))) # ray.put should be the identity

    # make sure objects are immutable, this example is why we need to copy
    # arguments before passing them into remote functions in python mode
    aref = test_functions.python_mode_f()
    self.assertTrue(np.alltrue(aref == np.array([0, 0])))
    bref = test_functions.python_mode_g(aref)
    self.assertTrue(np.alltrue(aref == np.array([0, 0]))) # python_mode_g should not mutate aref
    self.assertTrue(np.alltrue(bref == np.array([1, 0])))

    ray.services.cleanup()

class PythonCExtensionTest(unittest.TestCase):

  def testReferenceCountNone(self):
    ray.services.start_ray_local(num_workers=1)

    # Make sure that we aren't accidentally messing up Python's reference counts.
    for obj in [None, True, False]:
      @ray.remote([], [int])
      def f():
        return sys.getrefcount(obj)
      first_count = ray.get(f())
      second_count = ray.get(f())
      self.assertEqual(first_count, second_count)

    ray.services.cleanup()

class ReusablesTest(unittest.TestCase):

  def testReusables(self):
    ray.services.start_ray_local(num_workers=1)

    # Test that we can add a variable to the key-value store.

    def foo_initializer():
      return 1
    def foo_reinitializer(foo):
      return foo

    ray.reusables.foo = ray.Reusable(foo_initializer, foo_reinitializer)
    self.assertEqual(ray.reusables.foo, 1)

    @ray.remote([], [int])
    def use_foo():
      return ray.reusables.foo
    self.assertEqual(ray.get(use_foo()), 1)
    self.assertEqual(ray.get(use_foo()), 1)
    self.assertEqual(ray.get(use_foo()), 1)

    # Test that we can add a variable to the key-value store, mutate it, and reset it.

    def bar_initializer():
      return [1, 2, 3]

    ray.reusables.bar = ray.Reusable(bar_initializer)

    @ray.remote([], [list])
    def use_bar():
      ray.reusables.bar.append(4)
      return ray.reusables.bar
    self.assertEqual(ray.get(use_bar()), [1, 2, 3, 4])
    self.assertEqual(ray.get(use_bar()), [1, 2, 3, 4])
    self.assertEqual(ray.get(use_bar()), [1, 2, 3, 4])

    # Test that we can use the reinitializer.

    def baz_initializer():
      return np.zeros([4])
    def baz_reinitializer(baz):
      for i in range(len(baz)):
        baz[i] = 0
      return baz

    ray.reusables.baz = ray.Reusable(baz_initializer, baz_reinitializer)

    @ray.remote([int], [np.ndarray])
    def use_baz(i):
      baz = ray.reusables.baz
      baz[i] = 1
      return baz
    self.assertTrue(np.alltrue(ray.get(use_baz(0)) == np.array([1, 0, 0, 0])))
    self.assertTrue(np.alltrue(ray.get(use_baz(1)) == np.array([0, 1, 0, 0])))
    self.assertTrue(np.alltrue(ray.get(use_baz(2)) == np.array([0, 0, 1, 0])))
    self.assertTrue(np.alltrue(ray.get(use_baz(3)) == np.array([0, 0, 0, 1])))

    # Make sure the reinitializer is actually getting called. Note that this is
    # not the correct usage of a reinitializer because it does not reset qux to
    # its original state. This is just for testing.

    def qux_initializer():
      return 0
    def qux_reinitializer(x):
      return x + 1

    ray.reusables.qux = ray.Reusable(qux_initializer, qux_reinitializer)

    @ray.remote([], [int])
    def use_qux():
      return ray.reusables.qux
    self.assertEqual(ray.get(use_qux()), 0)
    self.assertEqual(ray.get(use_qux()), 1)
    self.assertEqual(ray.get(use_qux()), 2)

    ray.services.cleanup()

if __name__ == "__main__":
    unittest.main()
