from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


# TODO(ekl): implement common preprocessors
class Preprocessor(object):
  def output_shape(self):
    """Returns the new output shape, or None if unchanged."""
    raise NotImplementedError

  def preprocess(self, observation):
    """Returns the preprocessed observation."""
    raise NotImplementedError
