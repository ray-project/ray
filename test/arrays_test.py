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
import arrays.dist as dist

from google.protobuf.text_format import *

from grpc.beta import implementations
import orchestra_pb2
import types_pb2

class ArraysSingleTest(unittest.TestCase):

  def testMethods(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_cluster(return_drivers=False, num_workers_per_objstore=1, worker_path=test_path)

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
    [w] = services.start_cluster(return_drivers=True)

    x = dist.DistArray()
    x.construct([2, 3, 4], np.array([[[orchpy.push(0, w)]]]))
    capsule, _ = serialization.serialize(w.handle, x) # TODO(rkn): THIS REQUIRES A WORKER_HANDLE
    y = serialization.deserialize(w.handle, capsule) # TODO(rkn): THIS REQUIRES A WORKER_HANDLE
    self.assertEqual(x.shape, y.shape)
    self.assertEqual(x.objrefs[0, 0, 0].val, y.objrefs[0, 0, 0].val)

    services.cleanup()

  def testAssemble(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_cluster(return_drivers=False, num_workers_per_objstore=1, worker_path=test_path)

    a = single.ones([dist.BLOCK_SIZE, dist.BLOCK_SIZE], "float")
    b = single.zeros([dist.BLOCK_SIZE, dist.BLOCK_SIZE], "float")
    x = dist.DistArray()
    x.construct([2 * dist.BLOCK_SIZE, dist.BLOCK_SIZE], np.array([[a], [b]]))
    self.assertTrue(np.alltrue(x.assemble() == np.vstack([np.ones([dist.BLOCK_SIZE, dist.BLOCK_SIZE]), np.zeros([dist.BLOCK_SIZE, dist.BLOCK_SIZE])])))

    services.cleanup()

  def testMethods(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_cluster(return_drivers=False, num_objstores=2, num_workers_per_objstore=8, worker_path=test_path)

    x = dist.zeros([9, 25, 51], "float")
    y = dist.assemble(x)
    self.assertTrue(np.alltrue(orchpy.pull(y) == np.zeros([9, 25, 51])))

    x = dist.ones([11, 25, 49], "float")
    y = dist.assemble(x)
    self.assertTrue(np.alltrue(orchpy.pull(y) == np.ones([11, 25, 49])))

    x = dist.random.normal([11, 25, 49])
    y = dist.copy(x)
    z = dist.assemble(x)
    w = dist.assemble(y)
    self.assertTrue(np.alltrue(orchpy.pull(z) == orchpy.pull(w)))

    x = dist.eye(25, "float")
    y = dist.assemble(x)
    self.assertTrue(np.alltrue(orchpy.pull(y) == np.eye(25)))

    x = dist.random.normal([25, 49])
    y = dist.triu(x)
    z = dist.assemble(y)
    w = dist.assemble(x)
    self.assertTrue(np.alltrue(orchpy.pull(z) == np.triu(orchpy.pull(w))))

    x = dist.random.normal([25, 49])
    y = dist.tril(x)
    z = dist.assemble(y)
    w = dist.assemble(x)
    self.assertTrue(np.alltrue(orchpy.pull(z) == np.tril(orchpy.pull(w))))

    x = dist.random.normal([25, 49])
    y = dist.random.normal([49, 18])
    z = dist.dot(x, y)
    w = dist.assemble(z)
    u = dist.assemble(x)
    v = dist.assemble(y)
    np.allclose(orchpy.pull(w), np.dot(orchpy.pull(u), orchpy.pull(v)))
    self.assertTrue(np.allclose(orchpy.pull(w), np.dot(orchpy.pull(u), orchpy.pull(v))))

    # test add
    x = dist.random.normal([23, 42])
    y = dist.random.normal([23, 42])
    z = dist.add(x, y)
    z_full = dist.assemble(z)
    x_full = dist.assemble(x)
    y_full = dist.assemble(y)
    self.assertTrue(np.allclose(orchpy.pull(z_full), orchpy.pull(x_full) + orchpy.pull(y_full)))

    # test subtract
    x = dist.random.normal([33, 40])
    y = dist.random.normal([33, 40])
    z = dist.subtract(x, y)
    z_full = dist.assemble(z)
    x_full = dist.assemble(x)
    y_full = dist.assemble(y)
    self.assertTrue(np.allclose(orchpy.pull(z_full), orchpy.pull(x_full) - orchpy.pull(y_full)))

    # test transpose
    x = dist.random.normal([234, 432])
    y = dist.transpose(x)
    x_full = dist.assemble(x)
    y_full = dist.assemble(y)
    self.assertTrue(np.alltrue(orchpy.pull(x_full).T == orchpy.pull(y_full)))

    # test numpy_to_dist
    x = dist.random.normal([23, 45])
    y = dist.assemble(x)
    z = dist.numpy_to_dist(y)
    w = dist.assemble(z)
    x_full = dist.assemble(x)
    z_full = dist.assemble(z)
    self.assertTrue(np.alltrue(orchpy.pull(x_full) == orchpy.pull(z_full)))
    self.assertTrue(np.alltrue(orchpy.pull(y) == orchpy.pull(w)))

    # test dist.tsqr
    for shape in [[123, dist.BLOCK_SIZE], [7, dist.BLOCK_SIZE], [dist.BLOCK_SIZE, dist.BLOCK_SIZE], [dist.BLOCK_SIZE, 7], [10 * dist.BLOCK_SIZE, dist.BLOCK_SIZE]]:
      x = dist.random.normal(shape)
      K = min(shape)
      q, r = dist.linalg.tsqr(x)
      x_full = dist.assemble(x)
      x_val = orchpy.pull(x_full)
      q_full = dist.assemble(q)
      q_val = orchpy.pull(q_full)
      r_val = orchpy.pull(r)
      self.assertTrue(r_val.shape == (K, shape[1]))
      self.assertTrue(np.alltrue(r_val == np.triu(r_val)))
      self.assertTrue(np.allclose(x_val, np.dot(q_val, r_val)))
      self.assertTrue(np.allclose(np.dot(q_val.T, q_val), np.eye(K)))

    # test dist.linalg.modified_lu
    def test_modified_lu(d1, d2):
      print "testing dist_modified_lu with d1 = " + str(d1) + ", d2 = " + str(d2)
      assert d1 >= d2
      k = min(d1, d2)
      m = single.random.normal([d1, d2])
      q, r = single.linalg.qr(m)
      l, u, s = dist.linalg.modified_lu(dist.numpy_to_dist(q))
      q_val = orchpy.pull(q)
      r_val = orchpy.pull(r)
      l_full = dist.assemble(l)
      l_val = orchpy.pull(l_full)
      u_val = orchpy.pull(u)
      s_val = orchpy.pull(s)
      s_mat = np.zeros((d1, d2))
      for i in range(len(s_val)):
        s_mat[i, i] = s_val[i]
      self.assertTrue(np.allclose(q_val - s_mat, np.dot(l_val, u_val))) # check that q - s = l * u
      self.assertTrue(np.alltrue(np.triu(u_val) == u_val)) # check that u is upper triangular
      self.assertTrue(np.alltrue(np.tril(l_val) == l_val)) # check that l is lower triangular

    for d1, d2 in [(100, 100), (99, 98), (7, 5), (7, 7), (20, 7), (20, 10)]:
      test_modified_lu(d1, d2)

    # test dist_tsqr_hr
    def test_dist_tsqr_hr(d1, d2):
      print "testing dist_tsqr_hr with d1 = " + str(d1) + ", d2 = " + str(d2)
      a = dist.random.normal([d1, d2])
      y, t, y_top, r = dist.linalg.tsqr_hr(a)
      a_full = dist.assemble(a)
      a_val = orchpy.pull(a_full)
      y_full = dist.assemble(y)
      y_val = orchpy.pull(y_full)
      t_val = orchpy.pull(t)
      y_top_val = orchpy.pull(y_top)
      r_val = orchpy.pull(r)
      tall_eye = np.zeros((d1, min(d1, d2)))
      np.fill_diagonal(tall_eye, 1)
      q = tall_eye - np.dot(y_val, np.dot(t_val, y_top_val.T))
      self.assertTrue(np.allclose(np.dot(q.T, q), np.eye(min(d1, d2)))) # check that q.T * q = I
      self.assertTrue(np.allclose(np.dot(q, r_val), a_val)) # check that a = (I - y * t * y_top.T) * r

    for d1, d2 in [(123, dist.BLOCK_SIZE), (7, dist.BLOCK_SIZE), (dist.BLOCK_SIZE, dist.BLOCK_SIZE), (dist.BLOCK_SIZE, 7), (10 * dist.BLOCK_SIZE, dist.BLOCK_SIZE)]:
      test_dist_tsqr_hr(d1, d2)

    def test_dist_qr(d1, d2):
      print "testing qr with d1 = {}, and d2 = {}.".format(d1, d2)
      a = dist.random.normal([d1, d2])
      K = min(d1, d2)
      q, r = dist.linalg.qr(a)
      a_full = dist.assemble(a)
      q_full = dist.assemble(q)
      r_full = dist.assemble(r)
      a_val = orchpy.pull(a_full)
      q_val = orchpy.pull(q_full)
      r_val = orchpy.pull(r_full)

      self.assertTrue(q_val.shape == (d1, K))
      self.assertTrue(r_val.shape == (K, d2))
      self.assertTrue(np.allclose(np.dot(q_val.T, q_val), np.eye(K)))
      self.assertTrue(np.alltrue(r_val == np.triu(r_val)))
      self.assertTrue(np.allclose(a_val, np.dot(q_val, r_val)))

    for d1, d2 in [(123, dist.BLOCK_SIZE), (7, dist.BLOCK_SIZE), (dist.BLOCK_SIZE, dist.BLOCK_SIZE), (dist.BLOCK_SIZE, 7), (13, 21), (34, 35), (8, 7)]:
      test_dist_qr(d1, d2)
      test_dist_qr(d2, d1)
    for _ in range(20):
      d1 = np.random.randint(1, 35)
      d2 = np.random.randint(1, 35)
      test_dist_qr(d1, d2)

    services.cleanup()

if __name__ == '__main__':
    unittest.main()
