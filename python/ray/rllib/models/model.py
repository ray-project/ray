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
  """

  def __init__(self, inputs, num_outputs):
    self._inputs = inputs
    self._outputs, self._last_layer = self._init(inputs, num_outputs)

  def _init(self):
    """Builds and returns the output and last layer of the network."""
    raise NotImplementedError

  def inputs(self):
    """Returns the input placeholder for this model."""
    return self._inputs

  def outputs(self):
    """Returns the output tensor of this model."""
    return self._outputs

  def last_layer(self):
    """Returns the layer right before the output."""
    return self._last_layer
