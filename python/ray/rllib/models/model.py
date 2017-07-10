from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Model(object):
  """Defines an abstract network model for use with RLlib.

  Models convert input tensors to a number of output features. These features
  can then be interpreted by Distribution classes to determine e.g. agent
  action values.
  """

  def __init__(self, inputs, num_outputs):
    self.inputs = inputs
    self.outputs = self._init(inputs, num_outputs)

  def _init(self):
    """Initializes the model given self.inputs and self.num_outputs."""
    raise NotImplementedError

  def inputs(self):
    """Returns the input placeholder for this model."""
    return self.inputs

  def outputs(self):
    """Returns the output tensor of this model."""
    return self.outputs
