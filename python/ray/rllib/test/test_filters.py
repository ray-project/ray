from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import numpy as np

from ray.rllib.utils.filter import RunningStat, MeanStdFilter


class RunningStatTest(unittest.TestCase):
    def testRunningStat(self):
        for shp in ((), (3,), (3, 4)):
            li = []
            rs = RunningStat(shp)
            for _ in range(5):
                val = np.random.randn(*shp)
                rs.push(val)
                li.append(val)
                m = np.mean(li, axis=0)
                self.assertTrue(np.allclose(rs.mean, m))
                v = (np.square(m) if (len(li) == 1)
                     else np.var(li, ddof=1, axis=0))
                self.assertTrue(np.allclose(rs.var, v))

    def testCombiningStat(self):
        for shape in [(), (3,), (3, 4)]:
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
        for shape in [(), (3,), (3, 4, 4)]:
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
