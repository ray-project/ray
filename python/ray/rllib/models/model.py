from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Model(object):
    """Defines an abstract network model for use with RLlib.

    Models convert input tensors to a number of output features. These features
    can then be interpreted by ActionDistribution classes to determine
    e.g. agent action values.

    The last layer of the network can also be retrieved if the algorithm
    needs to further post-processing (e.g. Actor and Critic networks in A3C).

    Attributes:
        inputs (Tensor): The input placeholder for this model.
        outputs (Tensor): The output vector of this model.
        last_layer (Tensor): The network layer right before the model output.
    """

    def __init__(self, inputs, num_outputs, options, prefix=""):
        self.inputs = inputs
        self.prefix = prefix
        self.outputs, self.last_layer = self._init(
            inputs, num_outputs, options)

    def _init(self):
        """Builds and returns the output and last layer of the network."""
        raise NotImplementedError
