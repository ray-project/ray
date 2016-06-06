import unittest
import halo
import halo.serialization as serialization
import halo.services as services
import halo.worker as worker
import numpy as np
import time
import subprocess32 as subprocess
import os

import halo.arrays.remote as ra
import halo.arrays.distributed as da

from google.protobuf.text_format import *

from grpc.beta import implementations
import halo_pb2
import types_pb2

class ArraysSingleTest(unittest.TestCase):

  def testMethods(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_singlenode_cluster(return_drivers=False, num_workers_per_objstore=1, worker_path=test_path)

    # test eye
    ref = ra.eye(3)
    val = halo.pull(ref)
    self.assertTrue(np.alltrue(val == np.eye(3)))

    # test zeros
    ref = ra.zeros([3, 4, 5])
    val = halo.pull(ref)
    self.assertTrue(np.alltrue(val == np.zeros([3, 4, 5])))

    # test qr - pass by value
    val_a = np.random.normal(size=[10, 13])
    ref_q, ref_r = ra.linalg.qr(val_a)
    val_q = halo.pull(ref_q)
    val_r = halo.pull(ref_r)
    self.assertTrue(np.allclose(np.dot(val_q, val_r), val_a))

    # test qr - pass by objref
    a = ra.random.normal([10, 13])
    ref_q, ref_r = ra.linalg.qr(a)
    val_a = halo.pull(a)
    val_q = halo.pull(ref_q)
    val_r = halo.pull(ref_r)
    self.assertTrue(np.allclose(np.dot(val_q, val_r), val_a))

    services.cleanup()

class ArraysDistTest(unittest.TestCase):

  def testSerialization(self):
    [w] = services.start_singlenode_cluster(return_drivers=True)

    x = da.DistArray()
    x.construct([2, 3, 4], np.array([[[halo.push(0, w)]]]))
    capsule, _ = serialization.serialize(w.handle, x) # TODO(rkn): THIS REQUIRES A WORKER_HANDLE
    y = serialization.deserialize(w.handle, capsule) # TODO(rkn): THIS REQUIRES A WORKER_HANDLE
    self.assertEqual(x.shape, y.shape)
    self.assertEqual(x.objrefs[0, 0, 0].val, y.objrefs[0, 0, 0].val)

    services.cleanup()

  def testAssemble(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_singlenode_cluster(return_drivers=False, num_workers_per_objstore=1, worker_path=test_path)

    a = ra.ones([da.BLOCK_SIZE, da.BLOCK_SIZE])
    b = ra.zeros([da.BLOCK_SIZE, da.BLOCK_SIZE])
    x = da.DistArray()
    x.construct([2 * da.BLOCK_SIZE, da.BLOCK_SIZE], np.array([[a], [b]]))
    self.assertTrue(np.alltrue(x.assemble() == np.vstack([np.ones([da.BLOCK_SIZE, da.BLOCK_SIZE]), np.zeros([da.BLOCK_SIZE, da.BLOCK_SIZE])])))

    services.cleanup()

  def testMethods(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "testrecv.py")
    services.start_singlenode_cluster(return_drivers=False, num_objstores=2, num_workers_per_objstore=5, worker_path=test_path)

    x = da.zeros([9, 25, 51], "float")
    y = da.assemble(x)
    self.assertTrue(np.alltrue(halo.pull(y) == np.zeros([9, 25, 51])))

    x = da.ones([11, 25, 49], dtype_name="float")
    y = da.assemble(x)
    self.assertTrue(np.alltrue(halo.pull(y) == np.ones([11, 25, 49])))

    x = da.random.normal([11, 25, 49])
    y = da.copy(x)
    z = da.assemble(x)
    w = da.assemble(y)
    self.assertTrue(np.alltrue(halo.pull(z) == halo.pull(w)))

    x = da.eye(25, dtype_name="float")
    y = da.assemble(x)
    self.assertTrue(np.alltrue(halo.pull(y) == np.eye(25)))

    x = da.random.normal([25, 49])
    y = da.triu(x)
    z = da.assemble(y)
    w = da.assemble(x)
    self.assertTrue(np.alltrue(halo.pull(z) == np.triu(halo.pull(w))))

    x = da.random.normal([25, 49])
    y = da.tril(x)
    z = da.assemble(y)
    w = da.assemble(x)
    self.assertTrue(np.alltrue(halo.pull(z) == np.tril(halo.pull(w))))

    x = da.random.normal([25, 49])
    y = da.random.normal([49, 18])
    z = da.dot(x, y)
    w = da.assemble(z)
    u = da.assemble(x)
    v = da.assemble(y)
    np.allclose(halo.pull(w), np.dot(halo.pull(u), halo.pull(v)))
    self.assertTrue(np.allclose(halo.pull(w), np.dot(halo.pull(u), halo.pull(v))))

    # test add
    x = da.random.normal([23, 42])
    y = da.random.normal([23, 42])
    z = da.add(x, y)
    z_full = da.assemble(z)
    x_full = da.assemble(x)
    y_full = da.assemble(y)
    self.assertTrue(np.allclose(halo.pull(z_full), halo.pull(x_full) + halo.pull(y_full)))

    # test subtract
    x = da.random.normal([33, 40])
    y = da.random.normal([33, 40])
    z = da.subtract(x, y)
    z_full = da.assemble(z)
    x_full = da.assemble(x)
    y_full = da.assemble(y)
    self.assertTrue(np.allclose(halo.pull(z_full), halo.pull(x_full) - halo.pull(y_full)))

    # test transpose
    x = da.random.normal([234, 432])
    y = da.transpose(x)
    x_full = da.assemble(x)
    y_full = da.assemble(y)
    self.assertTrue(np.alltrue(halo.pull(x_full).T == halo.pull(y_full)))

    # test numpy_to_dist
    x = da.random.normal([23, 45])
    y = da.assemble(x)
    z = da.numpy_to_dist(y)
    w = da.assemble(z)
    x_full = da.assemble(x)
    z_full = da.assemble(z)
    self.assertTrue(np.alltrue(halo.pull(x_full) == halo.pull(z_full)))
    self.assertTrue(np.alltrue(halo.pull(y) == halo.pull(w)))

    # test da.tsqr
    for shape in [[123, da.BLOCK_SIZE], [7, da.BLOCK_SIZE], [da.BLOCK_SIZE, da.BLOCK_SIZE], [da.BLOCK_SIZE, 7], [10 * da.BLOCK_SIZE, da.BLOCK_SIZE]]:
      x = da.random.normal(shape)
      K = min(shape)
      q, r = da.linalg.tsqr(x)
      x_full = da.assemble(x)
      x_val = halo.pull(x_full)
      q_full = da.assemble(q)
      q_val = halo.pull(q_full)
      r_val = halo.pull(r)
      self.assertTrue(r_val.shape == (K, shape[1]))
      self.assertTrue(np.alltrue(r_val == np.triu(r_val)))
      self.assertTrue(np.allclose(x_val, np.dot(q_val, r_val)))
      self.assertTrue(np.allclose(np.dot(q_val.T, q_val), np.eye(K)))

    # test da.linalg.modified_lu
    def test_modified_lu(d1, d2):
      print "testing dist_modified_lu with d1 = " + str(d1) + ", d2 = " + str(d2)
      assert d1 >= d2
      k = min(d1, d2)
      m = ra.random.normal([d1, d2])
      q, r = ra.linalg.qr(m)
      l, u, s = da.linalg.modified_lu(da.numpy_to_dist(q))
      q_val = halo.pull(q)
      r_val = halo.pull(r)
      l_full = da.assemble(l)
      l_val = halo.pull(l_full)
      u_val = halo.pull(u)
      s_val = halo.pull(s)
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
      a = da.random.normal([d1, d2])
      y, t, y_top, r = da.linalg.tsqr_hr(a)
      a_full = da.assemble(a)
      a_val = halo.pull(a_full)
      y_full = da.assemble(y)
      y_val = halo.pull(y_full)
      t_val = halo.pull(t)
      y_top_val = halo.pull(y_top)
      r_val = halo.pull(r)
      tall_eye = np.zeros((d1, min(d1, d2)))
      np.fill_diagonal(tall_eye, 1)
      q = tall_eye - np.dot(y_val, np.dot(t_val, y_top_val.T))
      self.assertTrue(np.allclose(np.dot(q.T, q), np.eye(min(d1, d2)))) # check that q.T * q = I
      self.assertTrue(np.allclose(np.dot(q, r_val), a_val)) # check that a = (I - y * t * y_thalo.T) * r

    for d1, d2 in [(123, da.BLOCK_SIZE), (7, da.BLOCK_SIZE), (da.BLOCK_SIZE, da.BLOCK_SIZE), (da.BLOCK_SIZE, 7), (10 * da.BLOCK_SIZE, da.BLOCK_SIZE)]:
      test_dist_tsqr_hr(d1, d2)

    def test_dist_qr(d1, d2):
      print "testing qr with d1 = {}, and d2 = {}.".format(d1, d2)
      a = da.random.normal([d1, d2])
      K = min(d1, d2)
      q, r = da.linalg.qr(a)
      a_full = da.assemble(a)
      q_full = da.assemble(q)
      r_full = da.assemble(r)
      a_val = halo.pull(a_full)
      q_val = halo.pull(q_full)
      r_val = halo.pull(r_full)

      self.assertTrue(q_val.shape == (d1, K))
      self.assertTrue(r_val.shape == (K, d2))
      self.assertTrue(np.allclose(np.dot(q_val.T, q_val), np.eye(K)))
      self.assertTrue(np.alltrue(r_val == np.triu(r_val)))
      self.assertTrue(np.allclose(a_val, np.dot(q_val, r_val)))

    for d1, d2 in [(123, da.BLOCK_SIZE), (7, da.BLOCK_SIZE), (da.BLOCK_SIZE, da.BLOCK_SIZE), (da.BLOCK_SIZE, 7), (13, 21), (34, 35), (8, 7)]:
      test_dist_qr(d1, d2)
      test_dist_qr(d2, d1)
    for _ in range(20):
      d1 = np.random.randint(1, 35)
      d2 = np.random.randint(1, 35)
      test_dist_qr(d1, d2)

    services.cleanup()

if __name__ == '__main__':
    unittest.main()
