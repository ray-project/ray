import unittest
import orchpy
import orchpy.serialization as serialization
import orchpy.services as services
import numpy as np
import time
import subprocess32 as subprocess
import os

import arrays.single as single
import arrays.dist as dist

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

class ArraysSingleTest(unittest.TestCase):

  def testMethods(self):
    scheduler_port = new_scheduler_port()
    objstore_port = new_objstore_port()
    worker1_port = new_worker_port()
    worker2_port = new_worker_port()

    services.start_scheduler(address(IP_ADDRESS, scheduler_port))

    time.sleep(0.1)

    services.start_objstore(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port))

    time.sleep(0.2)

    orchpy.connect(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker1_port))

    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_worker(test_path, address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker2_port))

    time.sleep(0.2)

    # test eye
    ref = single.eye(3, "float")
    val = orchpy.pull(ref)
    self.assertTrue(np.alltrue(val == np.eye(3)))

    # test zeros
    ref = single.zeros([3, 4, 5], "float")
    val = orchpy.pull(ref)
    self.assertTrue(np.alltrue(val == np.zeros([3, 4, 5])))

    # test qr - pass by value
    val_a = np.random.normal(size=[10, 13])
    ref_q, ref_r = single.linalg.qr(val_a)
    val_q = orchpy.pull(ref_q)
    val_r = orchpy.pull(ref_r)
    self.assertTrue(np.allclose(np.dot(val_q, val_r), val_a))

    # test qr - pass by objref
    a = single.random.normal([10, 13])
    ref_q, ref_r = single.linalg.qr(a)
    val_a = orchpy.pull(a)
    val_q = orchpy.pull(ref_q)
    val_r = orchpy.pull(ref_r)
    self.assertTrue(np.allclose(np.dot(val_q, val_r), val_a))

    services.cleanup()

class ArraysDistTest(unittest.TestCase):

  def testSerialization(self):
    x = dist.DistArray()
    x.construct([2, 3, 4], np.array([[[orchpy.lib.ObjRef(0)]]]))
    capsule = serialization.serialize(x)
    y = serialization.deserialize(capsule)
    self.assertEqual(x.shape, y.shape)
    self.assertEqual(x.objrefs[0, 0, 0].val, y.objrefs[0, 0, 0].val)

  def testAssemble(self):
    scheduler_port = new_scheduler_port()
    objstore_port = new_objstore_port()
    worker1_port = new_worker_port()
    worker2_port = new_worker_port()

    services.start_scheduler(address(IP_ADDRESS, scheduler_port))

    time.sleep(0.1)

    services.start_objstore(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port))

    time.sleep(0.2)

    orchpy.connect(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker1_port))

    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_worker(test_path, address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker2_port))

    time.sleep(0.2)

    a = single.ones([dist.BLOCK_SIZE, dist.BLOCK_SIZE], "float")
    b = single.zeros([dist.BLOCK_SIZE, dist.BLOCK_SIZE], "float")
    x = dist.DistArray()
    x.construct([2 * dist.BLOCK_SIZE, dist.BLOCK_SIZE], np.array([[a], [b]]))
    self.assertTrue(np.alltrue(x.assemble() == np.vstack([np.ones([dist.BLOCK_SIZE, dist.BLOCK_SIZE]), np.zeros([dist.BLOCK_SIZE, dist.BLOCK_SIZE])])))

    services.cleanup()

  def testMethods(self):
    scheduler_port = new_scheduler_port()
    objstore_port = new_objstore_port()
    worker1_port = new_worker_port()
    worker2_port = new_worker_port()
    worker3_port = new_worker_port()
    worker4_port = new_worker_port()

    services.start_scheduler(address(IP_ADDRESS, scheduler_port))

    time.sleep(0.1)

    services.start_objstore(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port))

    time.sleep(0.2)

    orchpy.connect(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker1_port))

    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_worker(test_path, address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker2_port))
    services.start_worker(test_path, address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker3_port))
    services.start_worker(test_path, address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker4_port))

    time.sleep(0.2)

    x = dist.zeros([9, 25, 51], "float")
    self.assertTrue(np.alltrue(orchpy.pull(dist.assemble(x)) == np.zeros([9, 25, 51])))

    x = dist.ones([11, 25, 49], "float")
    self.assertTrue(np.alltrue(orchpy.pull(dist.assemble(x)) == np.ones([11, 25, 49])))

    x = dist.random.normal([11, 25, 49])
    y = dist.copy(x)
    self.assertTrue(np.alltrue(orchpy.pull(dist.assemble(x)) == orchpy.pull(dist.assemble(y))))

    x = dist.eye(25, "float")
    self.assertTrue(np.alltrue(orchpy.pull(dist.assemble(x)) == np.eye(25)))

    x = dist.random.normal([25, 49])
    y = dist.triu(x)
    self.assertTrue(np.alltrue(orchpy.pull(dist.assemble(y)) == np.triu(orchpy.pull(dist.assemble(x)))))

    x = dist.random.normal([25, 49])
    y = dist.tril(x)
    self.assertTrue(np.alltrue(orchpy.pull(dist.assemble(y)) == np.tril(orchpy.pull(dist.assemble(x)))))

    x = dist.random.normal([25, 49])
    y = dist.random.normal([49, 18])
    z = dist.dot(x, y)
    self.assertTrue(np.allclose(orchpy.pull(dist.assemble(z)), np.dot(orchpy.pull(dist.assemble(x)), orchpy.pull(dist.assemble(y)))))

    services.cleanup()

if __name__ == '__main__':
    unittest.main()
