import unittest
import ray
import ray.serialization as serialization
import ray.services as services
import ray.worker as worker
import numpy as np
import time
import subprocess32 as subprocess
import os

import test_functions
import ray.arrays.remote as ra
import ray.arrays.distributed as da

class SerializationTest(unittest.TestCase):

  def roundTripTest(self, worker, data):
    serialized, _ = serialization.serialize(worker.handle, data)
    result = serialization.deserialize(worker.handle, serialized)
    self.assertEqual(data, result)

  def numpyTypeTest(self, worker, typ):
    a = np.random.randint(0, 10, size=(100, 100)).astype(typ)
    b, _ = serialization.serialize(worker.handle, a)
    c = serialization.deserialize(worker.handle, b)
    self.assertTrue((a == c).all())

  def testSerialize(self):
    [w] = services.start_singlenode_cluster(return_drivers=True)

    self.roundTripTest(w, [1, "hello", 3.0])
    self.roundTripTest(w, 42)
    self.roundTripTest(w, "hello world")
    self.roundTripTest(w, 42.0)
    self.roundTripTest(w, (1.0, "hi"))
    self.roundTripTest(w, None)
    self.roundTripTest(w, (None, None))
    self.roundTripTest(w, ("hello", None))
    self.roundTripTest(w, True)
    self.roundTripTest(w, False)
    self.roundTripTest(w, (True, False))
    self.roundTripTest(w, {True: "hello", False: "world"})

    self.roundTripTest(w, {"hello" : "world", 1: 42, 1.0: 45})
    self.roundTripTest(w, {})

    a = np.zeros((100, 100))
    res, _ = serialization.serialize(w.handle, a)
    b = serialization.deserialize(w.handle, res)
    self.assertTrue((a == b).all())

    self.numpyTypeTest(w, 'int8')
    self.numpyTypeTest(w, 'uint8')
    # self.numpyTypeTest('int16') # TODO(pcm): implement this
    # self.numpyTypeTest('int32') # TODO(pcm): implement this
    self.numpyTypeTest(w, 'float32')
    self.numpyTypeTest(w, 'float64')

    ref0 = ray.push(0, w)
    ref1 = ray.push(0, w)
    ref2 = ray.push(0, w)
    ref3 = ray.push(0, w)

    a = np.array([[ref0, ref1], [ref2, ref3]])
    capsule, _ = serialization.serialize(w.handle, a)
    result = serialization.deserialize(w.handle, capsule)
    self.assertTrue((a == result).all())

    self.roundTripTest(w, ref0)
    self.roundTripTest(w, [ref0, ref1, ref2, ref3])
    self.roundTripTest(w, {'0': ref0, '1': ref1, '2': ref2, '3': ref3})
    self.roundTripTest(w, (ref0, 1))

    services.cleanup()

class ObjStoreTest(unittest.TestCase):

  # Test setting up object stores, transfering data between them and retrieving data to a client
  def testObjStore(self):
    [w1, w2] = services.start_singlenode_cluster(return_drivers=True, num_objstores=2, num_workers_per_objstore=0)

    # pushing and pulling an object shouldn't change it
    for data in ["h", "h" * 10000, 0, 0.0]:
      objref = ray.push(data, w1)
      result = ray.pull(objref, w1)
      self.assertEqual(result, data)

    # pushing an object, shipping it to another worker, and pulling it shouldn't change it
    for data in ["h", "h" * 10000, 0, 0.0, [1, 2, 3, "a", (1, 2)], ("a", ("b", 3))]:
      objref = worker.push(data, w1)
      result = worker.pull(objref, w2)
      self.assertEqual(result, data)

    # pushing an array, shipping it to another worker, and pulling it shouldn't change it
    for data in [np.zeros([10, 20]), np.random.normal(size=[45, 25])]:
      objref = worker.push(data, w1)
      result = worker.pull(objref, w2)
      self.assertTrue(np.alltrue(result == data))

    """
    # pulling multiple times shouldn't matter
    for data in [np.zeros([10, 20]), np.random.normal(size=[45, 25]), np.zeros([10, 20], dtype=np.dtype("float64")), np.zeros([10, 20], dtype=np.dtype("float32")), np.zeros([10, 20], dtype=np.dtype("int64")), np.zeros([10, 20], dtype=np.dtype("int32"))]:
      objref = worker.push(data, w1)
      result = worker.pull(objref, w2)
      result = worker.pull(objref, w2)
      result = worker.pull(objref, w2)
      self.assertTrue(np.alltrue(result == data))
    """

    # shipping a numpy array inside something else should be fine
    data = ("a", np.random.normal(size=[10, 10]))
    objref = worker.push(data, w1)
    result = worker.pull(objref, w2)
    self.assertTrue(data[0] == result[0])
    self.assertTrue(np.alltrue(data[1] == result[1]))

    # shipping a numpy array inside something else should be fine
    data = ["a", np.random.normal(size=[10, 10])]
    objref = worker.push(data, w1)
    result = worker.pull(objref, w2)
    self.assertTrue(data[0] == result[0])
    self.assertTrue(np.alltrue(data[1] == result[1]))

    services.cleanup()

class SchedulerTest(unittest.TestCase):

  def testRemoteTask(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "test_worker.py")
    [w] = services.start_singlenode_cluster(return_drivers=True, num_workers_per_objstore=1, worker_path=test_path)

    value_before = "test_string"
    objref = w.submit_task("test_functions.print_string", [value_before])

    time.sleep(0.2)

    value_after = ray.pull(objref[0], w)
    self.assertEqual(value_before, value_after)

    time.sleep(0.1)

    services.cleanup()

class WorkerTest(unittest.TestCase):

  def testPushPull(self):
    [w] = services.start_singlenode_cluster(return_drivers=True)

    for i in range(100):
      value_before = i * 10 ** 6
      objref = ray.push(value_before, w)
      value_after = ray.pull(objref, w)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = i * 10 ** 6 * 1.0
      objref = ray.push(value_before, w)
      value_after = ray.pull(objref, w)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = "h" * i
      objref = ray.push(value_before, w)
      value_after = ray.pull(objref, w)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = [1] * i
      objref = ray.push(value_before, w)
      value_after = ray.pull(objref, w)
      self.assertEqual(value_before, value_after)

    services.cleanup()

class APITest(unittest.TestCase):

  def testObjRefAliasing(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "test_worker.py")
    [w] = services.start_singlenode_cluster(return_drivers=True, num_workers_per_objstore=3, worker_path=test_path)

    objref = w.submit_task("test_functions.test_alias_f", [])
    self.assertTrue(np.alltrue(ray.pull(objref[0], w) == np.ones([3, 4, 5])))
    objref = w.submit_task("test_functions.test_alias_g", [])
    self.assertTrue(np.alltrue(ray.pull(objref[0], w) == np.ones([3, 4, 5])))
    objref = w.submit_task("test_functions.test_alias_h", [])
    self.assertTrue(np.alltrue(ray.pull(objref[0], w) == np.ones([3, 4, 5])))

    services.cleanup()

  def testKeywordArgs(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "test_worker.py")
    services.start_singlenode_cluster(return_drivers=False, num_workers_per_objstore=1, worker_path=test_path)

    x = test_functions.keyword_fct1(1)
    self.assertEqual(ray.pull(x), "1 hello")
    x = test_functions.keyword_fct1(1, "hi")
    self.assertEqual(ray.pull(x), "1 hi")
    x = test_functions.keyword_fct1(1, b="world")
    self.assertEqual(ray.pull(x), "1 world")

    x = test_functions.keyword_fct2(a="w", b="hi")
    self.assertEqual(ray.pull(x), "w hi")
    x = test_functions.keyword_fct2(b="hi", a="w")
    self.assertEqual(ray.pull(x), "w hi")
    x = test_functions.keyword_fct2(a="w")
    self.assertEqual(ray.pull(x), "w world")
    x = test_functions.keyword_fct2(b="hi")
    self.assertEqual(ray.pull(x), "hello hi")
    x = test_functions.keyword_fct2("w")
    self.assertEqual(ray.pull(x), "w world")
    x = test_functions.keyword_fct2("w", "hi")
    self.assertEqual(ray.pull(x), "w hi")

    x = test_functions.keyword_fct3(0, 1, c="w", d="hi")
    self.assertEqual(ray.pull(x), "0 1 w hi")
    x = test_functions.keyword_fct3(0, 1, d="hi", c="w")
    self.assertEqual(ray.pull(x), "0 1 w hi")
    x = test_functions.keyword_fct3(0, 1, c="w")
    self.assertEqual(ray.pull(x), "0 1 w world")
    x = test_functions.keyword_fct3(0, 1, d="hi")
    self.assertEqual(ray.pull(x), "0 1 hello hi")
    x = test_functions.keyword_fct3(0, 1)
    self.assertEqual(ray.pull(x), "0 1 hello world")

    services.cleanup()

  def testVariableNumberOfArgs(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "test_worker.py")
    services.start_singlenode_cluster(return_drivers=False, num_workers_per_objstore=1, worker_path=test_path)

    x = test_functions.varargs_fct1(0, 1, 2)
    self.assertEqual(ray.pull(x), "0 1 2")
    x = test_functions.varargs_fct2(0, 1, 2)
    self.assertEqual(ray.pull(x), "1 2")

    self.assertTrue(test_functions.kwargs_exception_thrown)
    self.assertTrue(test_functions.varargs_and_kwargs_exception_thrown)

    services.cleanup()

class ReferenceCountingTest(unittest.TestCase):

  def testDeallocation(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "test_worker.py")
    services.start_singlenode_cluster(return_drivers=False, num_workers_per_objstore=3, worker_path=test_path)

    x = test_functions.test_alias_f()
    ray.pull(x)
    time.sleep(0.1)
    objref_val = x.val
    self.assertTrue(ray.scheduler_info()["reference_counts"][objref_val] == 1)

    del x
    self.assertTrue(ray.scheduler_info()["reference_counts"][objref_val] == -1) # -1 indicates deallocated

    y = test_functions.test_alias_h()
    ray.pull(y)
    time.sleep(0.1)
    objref_val = y.val
    self.assertTrue(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [1, 0, 0])

    del y
    self.assertTrue(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [-1, -1, -1])

    z = da.zeros([da.BLOCK_SIZE, 2 * da.BLOCK_SIZE], "float")
    time.sleep(0.1)
    objref_val = z.val
    self.assertTrue(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [1, 1, 1])

    del z
    time.sleep(0.1)
    self.assertTrue(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [-1, -1, -1])

    x = ra.zeros([10, 10], "float")
    y = ra.zeros([10, 10], "float")
    z = ra.dot(x, y)
    objref_val = x.val
    time.sleep(0.1)
    self.assertTrue(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [1, 1, 1])

    del x
    time.sleep(0.1)
    self.assertTrue(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [-1, 1, 1])
    del y
    time.sleep(0.1)
    self.assertTrue(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [-1, -1, 1])
    del z
    time.sleep(0.1)
    self.assertTrue(ray.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [-1, -1, -1])

    services.cleanup()

if __name__ == '__main__':
    unittest.main()
