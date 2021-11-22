import numpy as np
import unittest

import ray
from ray.rllib.agents.cql.loss_functions import compute_vib_loss
from tensorflow.keras import backend as K
from ray.rllib.utils.framework import try_import_tf, try_import_tfp
tf = try_import_tf()

class LossFunctionTest(tf.test.TestCase):

    def setUp(self):
        super().setUp()

    def testLossValue(self):
        encoding_size = 200
        encoding = tf.distributions.Normal(np.zeros(encoding_size, dtype=np.float32),
                                           np.ones(encoding_size, dtype=np.float32))
        loss = compute_vib_loss(encoding, encoding_size)
        result = 0.0
        self.assertEqual(K.eval(loss), result)

    def testLossValueIncorrect(self):
        encoding_size = 200
        encoding = tf.distributions.Normal(np.ones(encoding_size, dtype=np.float32),
                                           np.ones(encoding_size, dtype=np.float32))
        loss = compute_vib_loss(encoding, encoding_size)
        result = 0.0
        self.assertNotEqual(K.eval(loss), result)

if __name__ == '__main__':
    tf.test.main()
