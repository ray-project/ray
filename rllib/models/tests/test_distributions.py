import unittest
import numpy as np

from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class TestDistributions(unittest.TestCase):
    def test_categorical(self):
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


if __name__ == "__main__":
    import unittest
    unittest.main(verbosity=1)
