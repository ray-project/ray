import unittest
import orchpy
import orchpy.serialization as serialization
import orchpy.services as services
import orchpy.worker as worker
import numpy as np
import time
import subprocess32 as subprocess
import os

import arrays.single as single

from google.protobuf.text_format import *

from grpc.beta import implementations
import orchestra_pb2
import types_pb2

IP_ADDRESS = "127.0.0.1"
TIMEOUT_SECONDS = 5

def connect_to_scheduler(host, port):
  channel = implementations.insecure_channel(host, port)
  return orchestra_pb2.beta_create_Scheduler_stub(channel)

def connect_to_objstore(host, port):
  channel = implementations.insecure_channel(host, port)
  return orchestra_pb2.beta_create_ObjStore_stub(channel)

def address(host, port):
  return host + ":" + str(port)

scheduler_port_counter = 0
def new_scheduler_port():
  global scheduler_port_counter
  scheduler_port_counter += 1
  return 10000 + scheduler_port_counter

worker_port_counter = 0
def new_worker_port():
  global worker_port_counter
  worker_port_counter += 1
  return 40000 + worker_port_counter

objstore_port_counter = 0
def new_objstore_port():
  global objstore_port_counter
  objstore_port_counter += 1
  return 20000 + objstore_port_counter

class SerializationTest(unittest.TestCase):

  def roundTripTest(self, data):
    serialized = serialization.serialize(data)
    result = serialization.deserialize(serialized)
    self.assertEqual(data, result)

  def numpyTypeTest(self, typ):
    a = np.random.randint(0, 10, size=(100, 100)).astype(typ)
    b = serialization.serialize(a)
    c = serialization.deserialize(b)
    self.assertTrue((a == c).all())

  def testSerialize(self):
    self.roundTripTest([1, "hello", 3.0])
    self.roundTripTest(42)
    self.roundTripTest("hello world")
    self.roundTripTest(42.0)
    self.roundTripTest((1.0, "hi"))

    a = np.zeros((100, 100))
    res = serialization.serialize(a)
    b = serialization.deserialize(res)
    self.assertTrue((a == b).all())

    self.numpyTypeTest('int8')
    self.numpyTypeTest('uint8')
    # self.numpyTypeTest('int16') # TODO(pcm): implement this
    # self.numpyTypeTest('int32') # TODO(pcm): implement this
    self.numpyTypeTest('float32')
    self.numpyTypeTest('float64')

    a = np.array([[orchpy.lib.ObjRef(0), orchpy.lib.ObjRef(1)], [orchpy.lib.ObjRef(41), orchpy.lib.ObjRef(42)]])
    capsule = serialization.serialize(a)
    result = serialization.deserialize(capsule)
    self.assertTrue((a == result).all())

class OrchPyLibTest(unittest.TestCase):

    def testOrchPyLib(self):
      scheduler_port = new_scheduler_port()
      objstore_port = new_objstore_port()
      worker_port = new_worker_port()

      services.start_scheduler(address(IP_ADDRESS, scheduler_port))

      time.sleep(0.1)

      services.start_objstore(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port))

      time.sleep(0.2)

      w = worker.Worker()

      orchpy.connect(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker_port), w)

      w.put_object(orchpy.lib.ObjRef(0), 'hello world')
      result = w.get_object(orchpy.lib.ObjRef(0))

      self.assertEqual(result, 'hello world')

      services.cleanup()

class ObjStoreTest(unittest.TestCase):

  # Test setting up object stores, transfering data between them and retrieving data to a client
  def testObjStore(self):
    scheduler_port = new_scheduler_port()
    objstore1_port = new_objstore_port()
    objstore2_port = new_objstore_port()
    worker1_port = new_worker_port()
    worker2_port = new_worker_port()

    services.start_scheduler(address(IP_ADDRESS, scheduler_port))

    time.sleep(0.1)

    services.start_objstore(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore1_port))
    services.start_objstore(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore2_port))

    time.sleep(0.2)

    scheduler_stub = connect_to_scheduler(IP_ADDRESS, scheduler_port)
    objstore1_stub = connect_to_objstore(IP_ADDRESS, objstore1_port)
    objstore2_stub = connect_to_objstore(IP_ADDRESS, objstore2_port)

    worker1 = worker.Worker()
    orchpy.connect(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore1_port), address(IP_ADDRESS, worker1_port), worker1)

    worker2 = worker.Worker()
    orchpy.connect(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore2_port), address(IP_ADDRESS, worker2_port), worker2)

    # pushing and pulling an object shouldn't change it
    for data in ["h", "h" * 10000, 0, 0.0]:
      objref = orchpy.push(data, worker1)
      result = orchpy.pull(objref, worker1)
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
    scheduler_port = new_scheduler_port()
    objstore_port = new_objstore_port()
    worker1_port = new_worker_port()
    worker2_port = new_worker_port()

    services.start_scheduler(address(IP_ADDRESS, scheduler_port))

    time.sleep(0.1)

    services.start_objstore(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port))

    time.sleep(0.2)

    scheduler_stub = connect_to_scheduler(IP_ADDRESS, scheduler_port)
    objstore_stub = connect_to_objstore(IP_ADDRESS, objstore_port)

    time.sleep(0.2)

    worker1 = worker.Worker()
    orchpy.connect(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker1_port), worker1)

    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_worker(test_path, address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker2_port))

    time.sleep(0.2)

    value_before = "test_string"
    objref = worker1.remote_call("__main__.print_string", [value_before])

    time.sleep(0.2)

    value_after = orchpy.pull(objref[0], worker1)
    self.assertEqual(value_before, value_after)

    time.sleep(0.1)

    reply = scheduler_stub.SchedulerDebugInfo(orchestra_pb2.SchedulerDebugInfoRequest(), TIMEOUT_SECONDS)

    services.cleanup()

class WorkerTest(unittest.TestCase):

  def testPushPull(self):
    scheduler_port = new_scheduler_port()
    objstore_port = new_objstore_port()
    worker1_port = new_worker_port()

    services.start_scheduler(address(IP_ADDRESS, scheduler_port))

    time.sleep(0.1)

    services.start_objstore(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port))

    time.sleep(0.2)

    worker1 = worker.Worker()
    orchpy.connect(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker1_port), worker1)

    for i in range(100):
      value_before = i * 10 ** 6
      objref = orchpy.push(value_before, worker1)
      value_after = orchpy.pull(objref, worker1)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = i * 10 ** 6 * 1.0
      objref = orchpy.push(value_before, worker1)
      value_after = orchpy.pull(objref, worker1)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = "h" * i
      objref = orchpy.push(value_before, worker1)
      value_after = orchpy.pull(objref, worker1)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = [1] * i
      objref = orchpy.push(value_before, worker1)
      value_after = orchpy.pull(objref, worker1)
      self.assertEqual(value_before, value_after)

    services.cleanup()

if __name__ == '__main__':
    unittest.main()
