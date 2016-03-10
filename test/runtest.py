import unittest
import orchpy.unison as unison
import orchpy.services as services
import orchpy.worker as worker
import numpy as np
import time
import subprocess32 as subprocess
import os

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
    serialized = orchpy.lib.serialize_object(data)
    result = orchpy.lib.deserialize_object(serialized)
    self.assertEqual(data, result)

  def testSerialize(self):
    data = [1, "hello", 3.0]
    self.roundTripTest(data)

    a = np.zeros((100, 100))
    res = orchpy.lib.serialize_object(a)
    b = orchpy.lib.deserialize_object(res)
    self.assertTrue((a == b).all())

class ObjStoreTest(unittest.TestCase):

  """Test setting up object stores, transfering data between them and retrieving data to a client"""
  def testObjStore(self):
    scheduler_port = new_scheduler_port()
    objstore1_port = new_objstore_port()
    objstore2_port = new_objstore_port()
    worker1_port = new_worker_port()
    worker2_port = new_worker_port()

    services.start_scheduler(IP_ADDRESS, scheduler_port)
    services.start_objstore(IP_ADDRESS, objstore1_port)
    services.start_objstore(IP_ADDRESS, objstore2_port)

    time.sleep(0.2)

    scheduler_stub = connect_to_scheduler(IP_ADDRESS, scheduler_port)
    objstore1_stub = connect_to_objstore(IP_ADDRESS, objstore1_port)
    objstore2_stub = connect_to_objstore(IP_ADDRESS, objstore2_port)

    worker1 = worker.Worker()
    worker1.connect(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, worker1_port), address(IP_ADDRESS, objstore1_port))

    worker2 = worker.Worker()
    worker2.connect(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, worker2_port), address(IP_ADDRESS, objstore2_port))

    # pushing and pulling an object shouldn't change it
    for data in ["h", "h" * 10000, 0, 0.0]:
      objref = worker1.push(data)
      result = worker1.pull(objref)
      self.assertEqual(result, data)

    # pushing an object, shipping it to another worker, and pulling it shouldn't change it
    for data in ["h", "h" * 10000, 0, 0.0]:
      objref = worker1.push(data)
      response = objstore1_stub.DeliverObj(orchestra_pb2.DeliverObjRequest(objref=objref.get_id(), objstore_address=address(IP_ADDRESS, objstore2_port)), TIMEOUT_SECONDS)
      result = worker2.pull(objref)
      self.assertEqual(result, data)

    services.cleanup()

class SchedulerTest(unittest.TestCase):

  def testCall(self):
    scheduler_port = new_scheduler_port()
    objstore_port = new_objstore_port()
    worker1_port = new_worker_port()
    worker2_port = new_worker_port()

    services.start_scheduler(IP_ADDRESS, scheduler_port)
    services.start_objstore(IP_ADDRESS, objstore_port)

    time.sleep(0.2)

    scheduler_stub = connect_to_scheduler(IP_ADDRESS, scheduler_port)
    objstore_stub = connect_to_objstore(IP_ADDRESS, objstore_port)

    time.sleep(0.2)

    worker1 = worker.Worker()
    worker1.connect(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, worker1_port), address(IP_ADDRESS, objstore_port))
    worker1.start_worker_service()

    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_worker(test_path, IP_ADDRESS, scheduler_port, worker2_port, objstore_port)

    time.sleep(0.2)

    worker1.call("hello_world", ["hi"])

    time.sleep(0.1)

    reply = scheduler_stub.GetDebugInfo(orchestra_pb2.GetDebugInfoRequest(), TIMEOUT_SECONDS)

    services.cleanup()

if __name__ == '__main__':
    unittest.main()
