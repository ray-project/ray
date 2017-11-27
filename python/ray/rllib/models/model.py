from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf


class Model(object):
    """Defines an abstract network model for use with RLlib.

    Models convert input tensors to a number of output features. These features
    can then be interpreted by ActionDistribution classes to determine
    e.g. agent action values.

    The last layer of the network can also be retrieved if the algorithm
    needs to further post-processing (e.g. Actor and Critic networks in A3C).

    If `options["free_log_std"]` is True, the last half of the
    output layer will be free variables that are not dependent on
    inputs. This is often used if the output of the network is used
    to parametrize a probability distribution. In this case, the
    first half of the parameters can be interpreted as a location
    parameter (like a mean) and the second half can be interpreted as
    a scale parameter (like a standard deviation).

    Attributes:
        inputs (Tensor): The input placeholder for this model.
        outputs (Tensor): The output vector of this model.
        last_layer (Tensor): The network layer right before the model output.
    """

    def __init__(self, inputs, num_outputs, options):
        self.inputs = inputs
        if options.get("free_log_std", False):
            assert num_outputs % 2 == 0
            num_outputs = num_outputs // 2
        self.outputs, self.last_layer = self._init(
            inputs, num_outputs, options)
        if options.get("free_log_std", False):
            log_std = tf.get_variable(name="log_std", shape=[num_outputs],
                                      initializer=tf.zeros_initializer)
            self.outputs = tf.concat(
                [self.outputs, 0.0 * self.outputs + log_std], 1)

    def _init(self):
        """Builds and returns the output and last layer of the network."""
        raise NotImplementedError
