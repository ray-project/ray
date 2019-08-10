from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import numpy as np
from numpy.testing import assert_allclose

from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.agents.ppo.utils import flatten, concatenate
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


# TODO(ekl): move to rllib/models dir
class DistributionsTest(unittest.TestCase):
    def testCategorical(self):
        num_samples = 100000
        logits = tf.placeholder(tf.float32, shape=(None, 10))
        z = 8 * (np.random.rand(10) - 0.5)
        data = np.tile(z, (num_samples, 1))
        c = Categorical(logits, {})  # dummy config dict
        sample_op = c.sample()
        sess = tf.Session()
        sess.run(tf.global_variables_initializer())
        samples = sess.run(sample_op, feed_dict={logits: data})
        counts = np.zeros(10)
        for sample in samples:
            counts[sample] += 1.0
        probs = np.exp(z) / np.sum(np.exp(z))
        self.assertTrue(np.sum(np.abs(probs - counts / num_samples)) <= 0.01)


class UtilsTest(unittest.TestCase):
    def testFlatten(self):
        d = {
            "s": np.array([[[1, -1], [2, -2]], [[3, -3], [4, -4]]]),
            "a": np.array([[[5], [-5]], [[6], [-6]]])
        }
        flat = flatten(d.copy(), start=0, stop=2)
        assert_allclose(d["s"][0][0][:], flat["s"][0][:])
        assert_allclose(d["s"][0][1][:], flat["s"][1][:])
        assert_allclose(d["s"][1][0][:], flat["s"][2][:])
        assert_allclose(d["s"][1][1][:], flat["s"][3][:])
        assert_allclose(d["a"][0][0], flat["a"][0])
        assert_allclose(d["a"][0][1], flat["a"][1])
        assert_allclose(d["a"][1][0], flat["a"][2])
        assert_allclose(d["a"][1][1], flat["a"][3])

    def testConcatenate(self):
        d1 = {"s": np.array([0, 1]), "a": np.array([2, 3])}
        d2 = {"s": np.array([4, 5]), "a": np.array([6, 7])}
        d = concatenate([d1, d2])
        assert_allclose(d["s"], np.array([0, 1, 4, 5]))
        assert_allclose(d["a"], np.array([2, 3, 6, 7]))

        D = concatenate([d])
        assert_allclose(D["s"], np.array([0, 1, 4, 5]))
        assert_allclose(D["a"], np.array([2, 3, 6, 7]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
