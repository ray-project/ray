from typing import List
import numpy as np
import arrays.single as single
import orchpy as op

BLOCK_SIZE = 10

class DistArray(object):
  def construct(self, shape, dtype, objrefs):
    self.shape = shape
    self.dtype = dtype
    self.objrefs = objrefs
    self.ndim = len(shape)
    self.num_blocks = [int(np.ceil(1.0 * a / BLOCK_SIZE)) for a in self.shape]
    if self.num_blocks != list(self.objrefs.shape):
      raise Exception("The fields `num_blocks` and `objrefs` are inconsistent, `num_blocks` is {} and `objrefs` has shape {}".format(self.num_blocks, list(self.objrefs.shape)))

  def deserialize(self, primitives):
    (shape, dtype_name, objrefs) = primitives
    self.construct(shape, np.dtype(dtype_name), objrefs)

  def serialize(self):
    return (self.shape, self.dtype.__name__, self.objrefs)

  def __init__(self):
    self.shape = None
    self.dtype = None
    self.objrefs = None

  def compute_block_lower(self, index):
    if len(index) != self.ndim:
      raise Exception("The value `index` equals {}, but `ndim` is {}.".format(index, self.ndim))
    return [elem * BLOCK_SIZE for elem in index]

  def compute_block_upper(self, index):
    if len(index) != self.ndim:
      raise Exception("The value `index` equals {}, but `ndim` is {}.".format(index, self.ndim))
    upper = []
    for i in range(self.ndim):
      upper.append(min((index[i] + 1) * BLOCK_SIZE, self.shape[i]))
    return upper

  def compute_block_shape(self, index):
    lower = self.compute_block_lower(index)
    upper = self.compute_block_upper(index)
    return [u - l for (l, u) in zip(lower, upper)]

  def assemble(self):
    """Assemble an array on this node from a distributed array object reference."""
    result = np.zeros(self.shape)
    for index in np.ndindex(*self.num_blocks):
      lower = self.compute_block_lower(index)
      upper = self.compute_block_upper(index)
      result[[slice(l, u) for (l, u) in zip(lower, upper)]] = op.pull(self.objrefs[index])
    return result

  def __getitem__(self, sliced):
    # TODO(rkn): fix this, this is just a placeholder that should work but is inefficient
    a = self.assemble()
    return a[sliced]
