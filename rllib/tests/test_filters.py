from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import numpy as np

import ray
from ray.rllib.utils.filter import RunningStat, MeanStdFilter
from ray.rllib.utils import FilterManager
from ray.rllib.tests.mock_worker import _MockWorker


class RunningStatTest(unittest.TestCase):
    def testRunningStat(self):
        for shp in ((), (3, ), (3, 4)):
            li = []
            rs = RunningStat(shp)
            for _ in range(5):
                val = np.random.randn(*shp)
                rs.push(val)
                li.append(val)
                m = np.mean(li, axis=0)
                self.assertTrue(np.allclose(rs.mean, m))
                v = (np.square(m)
                     if (len(li) == 1) else np.var(li, ddof=1, axis=0))
                self.assertTrue(np.allclose(rs.var, v))

    def testCombiningStat(self):
        for shape in [(), (3, ), (3, 4)]:
            li = []
            rs1 = RunningStat(shape)
            rs2 = RunningStat(shape)
            rs = RunningStat(shape)
            for _ in range(5):
                val = np.random.randn(*shape)
                rs1.push(val)
                rs.push(val)
                li.append(val)
            for _ in range(9):
                rs2.push(val)
                rs.push(val)
                li.append(val)
            rs1.update(rs2)
            assert np.allclose(rs.mean, rs1.mean)
            assert np.allclose(rs.std, rs1.std)


class MSFTest(unittest.TestCase):
    def testBasic(self):
        for shape in [(), (3, ), (3, 4, 4)]:
            filt = MeanStdFilter(shape)
            for i in range(5):
                filt(np.ones(shape))
            self.assertEqual(filt.rs.n, 5)
            self.assertEqual(filt.buffer.n, 5)

            filt2 = MeanStdFilter(shape)
            filt2.sync(filt)
            self.assertEqual(filt2.rs.n, 5)
            self.assertEqual(filt2.buffer.n, 5)

            filt.clear_buffer()
            self.assertEqual(filt.buffer.n, 0)
            self.assertEqual(filt2.buffer.n, 5)

            filt.apply_changes(filt2, with_buffer=False)
            self.assertEqual(filt.buffer.n, 0)
            self.assertEqual(filt.rs.n, 10)

            filt.apply_changes(filt2, with_buffer=True)
            self.assertEqual(filt.buffer.n, 5)
            self.assertEqual(filt.rs.n, 15)


class FilterManagerTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=1)

    def tearDown(self):
        ray.shutdown()

    def testSynchronize(self):
        """Synchronize applies filter buffer onto own filter"""
        filt1 = MeanStdFilter(())
        for i in range(10):
            filt1(i)
        self.assertEqual(filt1.rs.n, 10)
        filt1.clear_buffer()
        self.assertEqual(filt1.buffer.n, 0)

        RemoteWorker = ray.remote(_MockWorker)
        remote_e = RemoteWorker.remote(sample_count=10)
        remote_e.sample.remote()

        FilterManager.synchronize({
            "obs_filter": filt1,
            "rew_filter": filt1.copy()
        }, [remote_e])

        filters = ray.get(remote_e.get_filters.remote())
        obs_f = filters["obs_filter"]
        self.assertEqual(filt1.rs.n, 20)
        self.assertEqual(filt1.buffer.n, 0)
        self.assertEqual(obs_f.rs.n, filt1.rs.n)
        self.assertEqual(obs_f.buffer.n, filt1.buffer.n)


if __name__ == "__main__":
    unittest.main(verbosity=2)
