from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Model(object):
    """Your class must implement this interface to be used with Ray SGD.

    This supports any form of input pipeline: it is up to you to define it
    using TensorFlow.

    For an example implementation, see tfbench/test_model.py

    Attributes:
        loss (tf.Tensor): Loss function to minimize.
        optimizer (tf.train.Optimizer): Optimizer to use to minimize the loss.
    """

    def get_feed_dict(self):
        """Extra values to pass in when computing gradients for the loss."""
        return {}
