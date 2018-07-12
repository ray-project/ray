from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import random
import tensorflow as tf


def seed(seed_value):
    np.random.seed(seed_value)
    random.seed(seed_value)
    tf.set_random_seed(seed_value)
