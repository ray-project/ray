import unittest
import orchpy
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

    worker.connect(address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker1_port))

    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_worker(test_path, address(IP_ADDRESS, scheduler_port), address(IP_ADDRESS, objstore_port), address(IP_ADDRESS, worker2_port))

    time.sleep(0.2)

    # test eye
    ref = single.eye(3)
    time.sleep(0.2)
    val = orchpy.pull(ref)
    self.assertTrue(np.alltrue(val == np.eye(3)))

    # test zeros
    ref = single.zeros([3, 4, 5])
    time.sleep(0.2)
    val = orchpy.pull(ref)
    self.assertTrue(np.alltrue(val == np.zeros([3, 4, 5])))

    # test qr - pass by value
    val_a = np.random.normal(size=[10, 13])
    time.sleep(0.2)
    ref_q, ref_r = single.linalg.qr(val_a)
    time.sleep(0.2)
    val_q = orchpy.pull(ref_q)
    val_r = orchpy.pull(ref_r)
    self.assertTrue(np.allclose(np.dot(val_q, val_r), val_a))

    # test qr - pass by objref
    a = single.random.normal([10, 13])
    time.sleep(0.2) # TODO(rkn): fails without this sleep
    ref_q, ref_r = single.linalg.qr(a)
    time.sleep(0.2)
    val_a = orchpy.pull(a)
    val_q = orchpy.pull(ref_q)
    val_r = orchpy.pull(ref_r)
    self.assertTrue(np.allclose(np.dot(val_q, val_r), val_a))

    services.cleanup()

if __name__ == '__main__':
    unittest.main()
