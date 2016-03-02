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

"""
class UnisonTest(unittest.TestCase):

  def testSerialize(self):
    d = [1, 2L, "hello", 3.0]
    res = unison.serialize_args(d)
    c = unison.deserialize_args(res)
    self.assertEqual(c, d)

    d = [{'hello': 'world'}]
    res = unison.serialize_args(d)
    c = unison.deserialize_args(res)
    self.assertEqual(c, d)

    a = np.zeros((100, 100))
    res = unison.serialize_args(a)
    b = unison.deserialize_args(res)
    self.assertTrue((a == b).all())

    a = [unison.ObjRef(42, int)]
    res = unison.serialize_args(a)
    b = unison.deserialize_args(res)
    self.assertEqual(a, b)
"""

TIMEOUT_SECONDS = 5

def produce_data(num_chunks):
  for _ in range(num_chunks):
    yield orchestra_pb2.ObjChunk(objref=1, totalsize=1000, data=b"hello world")

def connect_to_scheduler(host, port):
  channel = implementations.insecure_channel(host, port)
  return orchestra_pb2.beta_create_Scheduler_stub(channel)

def connect_to_objstore(host, port):
  channel = implementations.insecure_channel(host, port)
  return orchestra_pb2.beta_create_ObjStore_stub(channel)

class ObjStoreTest(unittest.TestCase):

  """Test setting up object stores, transfering data between them and retrieving data to a client"""
  def testObjStore(self):
    services.start_scheduler("0.0.0.0:22221")
    services.start_objstore("0.0.0.0:22222")
    services.start_objstore("0.0.0.0:22223")

    time.sleep(0.2)

    scheduler_stub = connect_to_scheduler('localhost', 22221)
    objstore1_stub = connect_to_objstore('localhost', 22222)
    objstore2_stub = connect_to_objstore('localhost', 22223)

    worker1 = worker.Worker()
    worker1.connect("127.0.0.1:22221", "127.0.0.1:40000", "127.0.0.1:22222")

    worker2 = worker.Worker()
    worker2.connect("127.0.0.1:22221", "127.0.0.1:40001", "127.0.0.1:22223")

    for i in range(1, 100):
        l = i * 100 * "h"
        objref = worker1.push(l)
        response = objstore1_stub.DeliverObj(orchestra_pb2.DeliverObjRequest(objref=objref, objstore_address="0.0.0.0:22223"), TIMEOUT_SECONDS)
        s = worker2.get_serialized(objref)
        result = worker.unison.deserialize_from_string(s)
        self.assertEqual(len(result), 100 * i)

    services.cleanup()

class SchedulerTest(unittest.TestCase):

  def testCall(self):
    services.start_scheduler("0.0.0.0:22221")
    services.start_objstore("0.0.0.0:22222")

    time.sleep(0.2)

    scheduler_stub = connect_to_scheduler('localhost', 22221)
    objstore_stub = connect_to_objstore('localhost', 22222)

    time.sleep(0.2)

    w = worker.Worker()
    w.connect("127.0.0.1:22221", "127.0.0.1:40003", "127.0.0.1:22222")
    w2 = worker.Worker()
    w2.connect("127.0.0.1:22221", "127.0.0.1:40004", "127.0.0.1:22222")

    time.sleep(0.2)

    w.register_function("hello_world", None, 2)
    w2.register_function("hello_world", None, 2)

    time.sleep(0.1)

    w.call("hello_world", ["hi"])

    time.sleep(0.1)

    reply = scheduler_stub.GetDebugInfo(orchestra_pb2.GetDebugInfoRequest(), TIMEOUT_SECONDS)

    self.assertEqual(reply.task[0].name, u'hello_world')

    test_path = os.path.dirname(os.path.abspath(__file__))

    p = subprocess.Popen(["python", os.path.join(test_path, "testrecv.py")])

    time.sleep(0.2)

    scheduler_stub.PushObj(orchestra_pb2.PushObjRequest(workerid=0), TIMEOUT_SECONDS)

    reply = scheduler_stub.GetDebugInfo(orchestra_pb2.GetDebugInfoRequest(do_scheduling=True), TIMEOUT_SECONDS)

    self.assertEqual(p.wait(), 0, "argument was not received by the test program")

    # w.main_loop()
    # w2.main_loop()
    #
    # reply = scheduler_stub.GetDebugInfo(orchestra_pb2.GetDebugInfoRequest(do_scheduling=True), TIMEOUT_SECONDS)
    # time.sleep(0.1)
    # reply = scheduler_stub.GetDebugInfo(orchestra_pb2.GetDebugInfoRequest(), TIMEOUT_SECONDS)
    #
    # self.assertEqual(list(reply.task), [])
    #
    # services.cleanup()


if __name__ == '__main__':
    unittest.main()
