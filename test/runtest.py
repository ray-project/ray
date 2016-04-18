import unittest
import orchpy
import orchpy.serialization as serialization
import orchpy.services as services
import orchpy.worker as worker
import numpy as np
import time
import subprocess32 as subprocess
import os

from google.protobuf.text_format import *

import orchestra_pb2
import types_pb2

import test_functions
import arrays.single as single
import arrays.dist as dist

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
    w = worker.Worker()
    services.start_cluster(driver_worker=w)

    self.roundTripTest(w, [1, "hello", 3.0])
    self.roundTripTest(w, 42)
    self.roundTripTest(w, "hello world")
    self.roundTripTest(w, 42.0)
    self.roundTripTest(w, (1.0, "hi"))

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

    ref0 = orchpy.push(0, w)
    ref1 = orchpy.push(0, w)
    ref2 = orchpy.push(0, w)
    ref3 = orchpy.push(0, w)
    a = np.array([[ref0, ref1], [ref2, ref3]])
    capsule, _ = serialization.serialize(w.handle, a)
    result = serialization.deserialize(w.handle, capsule)
    self.assertTrue((a == result).all())

    services.cleanup()

class ObjStoreTest(unittest.TestCase):

  # Test setting up object stores, transfering data between them and retrieving data to a client
  def testObjStore(self):
    w = worker.Worker()
    services.start_cluster(driver_worker=w)

    # pushing and pulling an object shouldn't change it
    for data in ["h", "h" * 10000, 0, 0.0]:
      objref = orchpy.push(data, w)
      result = orchpy.pull(objref, w)
      self.assertEqual(result, data)

    # pushing an object, shipping it to another worker, and pulling it shouldn't change it
    # for data in ["h", "h" * 10000, 0, 0.0]:
    #   objref = worker.push(data, worker1)
    #   response = objstore1_stub.DeliverObj(orchestra_pb2.DeliverObjRequest(objref=objref.val, objstore_address=address(IP_ADDRESS, objstore2_port)), TIMEOUT_SECONDS)
    #   result = worker.pull(objref, worker2)
    #   self.assertEqual(result, data)

    services.cleanup()

class SchedulerTest(unittest.TestCase):

  def testCall(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    w = worker.Worker()
    services.start_cluster(driver_worker=w, num_workers=1, worker_path=test_path)

    value_before = "test_string"
    objref = w.remote_call("test_functions.print_string", [value_before])

    time.sleep(0.2)

    value_after = orchpy.pull(objref[0], w)
    self.assertEqual(value_before, value_after)

    time.sleep(0.1)

    services.cleanup()

class WorkerTest(unittest.TestCase):

  def testPushPull(self):
    w = worker.Worker()
    services.start_cluster(driver_worker=w)

    for i in range(100):
      value_before = i * 10 ** 6
      objref = orchpy.push(value_before, w)
      value_after = orchpy.pull(objref, w)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = i * 10 ** 6 * 1.0
      objref = orchpy.push(value_before, w)
      value_after = orchpy.pull(objref, w)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = "h" * i
      objref = orchpy.push(value_before, w)
      value_after = orchpy.pull(objref, w)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = [1] * i
      objref = orchpy.push(value_before, w)
      value_after = orchpy.pull(objref, w)
      self.assertEqual(value_before, value_after)

    services.cleanup()

class APITest(unittest.TestCase):

  def testObjRefAliasing(self):
    w = worker.Worker()
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_cluster(num_workers=3, worker_path=test_path, driver_worker=w)

    objref = w.remote_call("test_functions.test_alias_f", [])
    self.assertTrue(np.alltrue(orchpy.pull(objref[0], w) == np.ones([3, 4, 5])))
    objref = w.remote_call("test_functions.test_alias_g", [])
    self.assertTrue(np.alltrue(orchpy.pull(objref[0], w) == np.ones([3, 4, 5])))
    objref = w.remote_call("test_functions.test_alias_h", [])
    self.assertTrue(np.alltrue(orchpy.pull(objref[0], w) == np.ones([3, 4, 5])))

    services.cleanup()

class ReferenceCountingTest(unittest.TestCase):

  def testDeallocation(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_cluster(num_workers=3, worker_path=test_path)

    x = test_functions.test_alias_f()
    orchpy.pull(x)
    time.sleep(0.1)
    objref_val = x.val
    self.assertTrue(orchpy.scheduler_info()["reference_counts"][objref_val] == 1)

    del x
    self.assertTrue(orchpy.scheduler_info()["reference_counts"][objref_val] == -1) # -1 indicates deallocated

    y = test_functions.test_alias_h()
    orchpy.pull(y)
    time.sleep(0.1)
    objref_val = y.val
    self.assertTrue(orchpy.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [1, 0, 0])

    del y
    self.assertTrue(orchpy.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [-1, -1, -1])

    z = dist.zeros([dist.BLOCK_SIZE, 2 * dist.BLOCK_SIZE], "float")
    time.sleep(0.1)
    objref_val = z.val
    self.assertTrue(orchpy.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [1, 1, 1])

    del z
    time.sleep(0.1)
    self.assertTrue(orchpy.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [-1, -1, -1])

    x = single.zeros([10, 10], "float")
    y = single.zeros([10, 10], "float")
    z = single.dot(x, y)
    objref_val = x.val
    time.sleep(0.1)
    self.assertTrue(orchpy.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [1, 1, 1])

    del x
    time.sleep(0.1)
    self.assertTrue(orchpy.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [-1, 1, 1])
    del y
    time.sleep(0.1)
    self.assertTrue(orchpy.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [-1, -1, 1])
    del z
    time.sleep(0.1)
    self.assertTrue(orchpy.scheduler_info()["reference_counts"][objref_val:(objref_val + 3)] == [-1, -1, -1])

    services.cleanup()

if __name__ == '__main__':
    unittest.main()
