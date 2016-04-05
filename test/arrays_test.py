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

class ArraysSingleTest(unittest.TestCase):

  def testMethods(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_cluster(num_workers=1, worker_path=test_path)

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
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_cluster(num_workers=1, worker_path=test_path)

    a = single.ones([dist.BLOCK_SIZE, dist.BLOCK_SIZE], "float")
    b = single.zeros([dist.BLOCK_SIZE, dist.BLOCK_SIZE], "float")
    x = dist.DistArray()
    x.construct([2 * dist.BLOCK_SIZE, dist.BLOCK_SIZE], np.array([[a], [b]]))
    self.assertTrue(np.alltrue(x.assemble() == np.vstack([np.ones([dist.BLOCK_SIZE, dist.BLOCK_SIZE]), np.zeros([dist.BLOCK_SIZE, dist.BLOCK_SIZE])])))

    services.cleanup()

  def testMethods(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_cluster(num_workers=3, worker_path=test_path)

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
