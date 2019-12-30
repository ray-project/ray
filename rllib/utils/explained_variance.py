from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils import try_import_tf, try_import_torch

tf = try_import_tf()
torch, nn = try_import_torch()


def explained_variance(y, pred, framework="tf"):
    if framework == "tf":
        _, y_var = tf.nn.moments(y, axes=[0])
        _, diff_var = tf.nn.moments(y - pred, axes=[0])
        return tf.maximum(-1.0, 1 - (diff_var / y_var))
    else:
        y_var = torch.var(y, dim=[0])
        diff_var = torch.var(y - pred, dim=[0])
        return max(-1.0, 1 - (diff_var / y_var))
