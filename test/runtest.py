import unittest
import orchpy.unison as unison
import orchpy.services as services
import orchpy.worker as worker
import numpy as np
import time

from grpc.beta import implementations
import orchestra_pb2
import types_pb2

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

TIMEOUT_SECONDS = 5

def produce_data(num_chunks):
  for _ in range(num_chunks):
    yield orchestra_pb2.ObjChunk(objref=1, totalsize=1000, data=b"hello world")

class ObjStoreTest(unittest.TestCase):

  """Test setting up object stores, transfering data between them and retrieving data to a client"""
  def testObjStore(self):
    services.start_scheduler("0.0.0.0:22221")
    services.start_objstore("0.0.0.0:22222")
    services.start_objstore("0.0.0.0:22223")
    time.sleep(0.5)

    scheduler_channel = implementations.insecure_channel('localhost', 22221)
    scheduler_stub = orchestra_pb2.beta_create_SchedulerServer_stub(scheduler_channel)
    objstore1_channel = implementations.insecure_channel('localhost', 22222)
    objstore1_stub = orchestra_pb2.beta_create_ObjStore_stub(objstore1_channel)
    objstore2_channel = implementations.insecure_channel('localhost', 22223)
    objstore2_stub = orchestra_pb2.beta_create_ObjStore_stub(objstore2_channel)

    scheduler_stub.RegisterObjStore(orchestra_pb2.RegisterObjStoreRequest(address="127.0.0.1:22222"), TIMEOUT_SECONDS)
    scheduler_stub.RegisterObjStore(orchestra_pb2.RegisterObjStoreRequest(address="127.0.0.1:22223"), TIMEOUT_SECONDS)

    worker.global_worker.connect("127.0.0.1:22221", "127.0.0.1:40000", "127.0.0.1:22222")

    other_worker = worker.Worker()
    other_worker.connect("127.0.0.1:22221", "127.0.0.1:40001", "127.0.0.1:22223")

    # import IPython
    # IPython.embed()

    for i in range(1, 10):
        l = i * 100 * "h"
        objref = worker.global_worker.do_push(l)
        # time.sleep(5.0)
        response = objstore1_stub.DeliverObj(orchestra_pb2.DeliverObjRequest(objref=objref, objstore_address="0.0.0.0:22223"), TIMEOUT_SECONDS)
        # time.sleep(5.0)
        str = other_worker.get_serialized(objref)
        result = worker.unison.deserialize_from_string(str)
        # import IPython
        # IPython.embed()
        self.assertEqual(len(result), 100 * i)

class SchedulerTest(unittest.TestCase):

  def testRegister(self):
    scheduler_channel = implementations.insecure_channel('localhost', 22221)
    scheduler_stub = orchestra_pb2.beta_create_SchedulerServer_stub(scheduler_channel)
    w = worker.Worker()
    w.connect("127.0.0.1:22221", "127.0.0.1:40002", "127.0.0.1:22222")
    w.register_function("hello_world", 2)


"""
class SchedulerTest(unittest.TestCase):

  def testServer(self):
    services.start_scheduler("0.0.0.0:22221")
    services.start_objstore("0.0.0.0:22222")
    services.start_objstore("0.0.0.0:22223")
    time.sleep(1.0)

    scheduler_channel = implementations.insecure_channel('localhost', 22221)
    scheduler_stub = orchestra_pb2.beta_create_SchedulerServer_stub(scheduler_channel)
    objstore_channel = implementations.insecure_channel('localhost', 22222)
    objstore_stub = orchestra_pb2.beta_create_ObjStore_stub(objstore_channel)
    objstore_channel2 = implementations.insecure_channel('localhost', 22223)
    objstore_stub2 = orchestra_pb2.beta_create_ObjStore_stub(objstore_channel2)

    # call = types_pb2.Call(name="test")
    # response = scheduler_stub.RemoteCall(orchestra_pb2.RemoteCallRequest(call=call), TIMEOUT_SECONDS)
    # response = scheduler_stub.RegisterFunction(orchestra_pb2.RegisterFunctionRequest(workerid=1, fnname="hello"), TIMEOUT_SECONDS)

    response2 = scheduler_stub.RegisterObjStore(orchestra_pb2.RegisterObjStoreRequest(address="127.0.0.1:22222"), TIMEOUT_SECONDS)
    response2 = scheduler_stub.RegisterObjStore(orchestra_pb2.RegisterObjStoreRequest(address="127.0.0.1:22223"), TIMEOUT_SECONDS)

    # response2 = scheduler_stub.RegisterObjStore(orchestra_pb2.RegisterObjStoreRequest(address="127.0.0.1:22222"), TIMEOUT_SECONDS)
    # response3 = scheduler_stub.RegisterObjStore(orchestra_pb2.RegisterObjStoreRequest(address="127.0.0.1:22223"), TIMEOUT_SECONDS)

    # objstore_stub.StreamObj(produce_data(100), TIMEOUT_SECONDS)

    worker.global_worker.connect("127.0.0.1:22221", "127.0.0.1:40000", "127.0.0.1:22222")

    l = [1, 2, 3, 4]
    worker.global_worker.do_push(l)

    ## res = scheduler_stub.PushObj(orchestra_pb2.PushObjRequest(workerid=0), TIMEOUT_SECONDS)

    response = objstore_stub.DeliverObj(orchestra_pb2.DeliverObjRequest(objref=0, objstore_address="0.0.0.0:22223"), TIMEOUT_SECONDS)

    # res = objstore_stub2.DebugInfo(orchestra_pb2.DebugInfoRequest(), TIMEOUT_SECONDS)

    response = objstore_stub.GetObj(orchestra_pb2.GetObjRequest(objref=0), TIMEOUT_SECONDS)

    worker.global_worker.get_serialized(0)

    import IPython
    IPython.embed()

    l = [1, 2, 3, 4]
    worker.global_worker.do_push(l)

    response = objstore_stub.DeliverObj(orchestra_pb2.DeliverObjRequest(), TIMEOUT_SECONDS)

    # response = objstore_stub.DebugInfo(orchestra_pb2.DebugInfoRequest(), TIMEOUT_SECONDS)

    # import IPython
    # IPython.embed()

    # worker.global_worker.connect("127.0.0.1:22221", "127.0.0.1:22222")
    # l = [1, 2, 3, 4]
    # worker.global_worker.do_push(l)

    # import IPython
    # IPython.embed()
    # response = objstore_stub.DeliverObj(orchestra_pb2.DeliverObjRequest())
    # print "Greeter client received: " + response.message
    # import IPython
    # IPython.embed()
"""

if __name__ == '__main__':
    unittest.main()
