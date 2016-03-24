from typing import List
import numpy as np
import arrays.single as single
import orchpy as op

__all__ = ["BLOCK_SIZE", "DistArray", "assemble", "zeros", "ones", "copy",
           "eye", "triu", "tril", "blockwise_dot", "dot", "block_column", "block_row"]

BLOCK_SIZE = 10

class DistArray(object):
  def construct(self, shape, objrefs):
    self.shape = shape
    self.objrefs = objrefs
    self.ndim = len(shape)
    self.num_blocks = [int(np.ceil(1.0 * a / BLOCK_SIZE)) for a in self.shape]
    if self.num_blocks != list(self.objrefs.shape):
      raise Exception("The fields `num_blocks` and `objrefs` are inconsistent, `num_blocks` is {} and `objrefs` has shape {}".format(self.num_blocks, list(self.objrefs.shape)))

  def deserialize(self, primitives):
    (shape, objrefs) = primitives
    self.construct(shape, objrefs)

  def serialize(self):
    return (self.shape, self.objrefs)

  def __init__(self):
    self.shape = None
    self.objrefs = None

  @staticmethod
  def compute_block_lower(index, shape):
    # TODO(rkn): Check that the entries of index are in the correct range.
    # TODO(rkn): Check that len(index) == len(shape).
    return [elem * BLOCK_SIZE for elem in index]

  @staticmethod
  def compute_block_upper(index, shape):
    # TODO(rkn): Check that the entries of index are in the correct range.
    # TODO(rkn): Check that len(index) == len(shape).
    upper = []
    for i in range(len(shape)):
      upper.append(min((index[i] + 1) * BLOCK_SIZE, shape[i]))
    return upper

  @staticmethod
  def compute_block_shape(index, shape):
    lower = DistArray.compute_block_lower(index, shape)
    upper = DistArray.compute_block_upper(index, shape)
    return [u - l for (l, u) in zip(lower, upper)]

  @staticmethod
  def compute_num_blocks(shape):
    return [int(np.ceil(1.0 * a / BLOCK_SIZE)) for a in shape]

  def assemble(self):
    """Assemble an array on this node from a distributed array object reference."""
    first_block = op.pull(self.objrefs[(0,) * self.ndim])
    dtype = first_block.dtype
    result = np.zeros(self.shape, dtype=dtype)
    for index in np.ndindex(*self.num_blocks):
      lower = DistArray.compute_block_lower(index, self.shape)
      upper = DistArray.compute_block_upper(index, self.shape)
      result[[slice(l, u) for (l, u) in zip(lower, upper)]] = op.pull(self.objrefs[index])
    return result

  def __getitem__(self, sliced):
    # TODO(rkn): fix this, this is just a placeholder that should work but is inefficient
    a = self.assemble()
    return a[sliced]

@op.distributed([DistArray], [np.ndarray])
def assemble(a):
  return a.assemble()

@op.distributed([List[int], str], [DistArray])
def zeros(shape, dtype_name):
  num_blocks = DistArray.compute_num_blocks(shape)
  objrefs = np.empty(num_blocks, dtype=object)
  for index in np.ndindex(*num_blocks):
    objrefs[index] = single.zeros(DistArray.compute_block_shape(index, shape), dtype_name)
  result = DistArray()
  result.construct(shape, objrefs)
  return result

@op.distributed([List[int], str], [DistArray])
def ones(shape, dtype_name):
  num_blocks = DistArray.compute_num_blocks(shape)
  objrefs = np.empty(num_blocks, dtype=object)
  for index in np.ndindex(*num_blocks):
    objrefs[index] = single.ones(DistArray.compute_block_shape(index, shape), dtype_name)
  result = DistArray()
  result.construct(shape, objrefs)
  return result

@op.distributed([DistArray], [DistArray])
def copy(a):
  num_blocks = DistArray.compute_num_blocks(a.shape)
  objrefs = np.empty(num_blocks, dtype=object)
  for index in np.ndindex(*num_blocks):
    objrefs[index] = single.copy(a.objrefs[index])
  result = DistArray()
  result.construct(a.shape, objrefs)
  return result

@op.distributed([int, str], [DistArray])
def eye(dim, dtype_name):
  shape = [dim, dim]
  num_blocks = DistArray.compute_num_blocks(shape)
  objrefs = np.empty(num_blocks, dtype=object)
  for (i, j) in np.ndindex(*num_blocks):
    if i == j:
      objrefs[i, j] = single.eye(DistArray.compute_block_shape([i, j], shape)[0], dtype_name)
    else:
      objrefs[i, j] = single.zeros(DistArray.compute_block_shape([i, j], shape), dtype_name)
  result = DistArray()
  result.construct(shape, objrefs)
  return result

@op.distributed([DistArray], [DistArray])
def triu(a):
  if a.ndim != 2:
    raise Exception("Input must have 2 dimensions, but a.ndim is " + str(a.ndim))
  objrefs = np.empty(a.num_blocks, dtype=object)
  for i in range(a.num_blocks[0]):
    for j in range(a.num_blocks[1]):
      if i < j:
        objrefs[i, j] = single.copy(a.objrefs[i, j])
      elif i == j:
        objrefs[i, j] = single.triu(a.objrefs[i, j])
      else:
        objrefs[i, j] = single.zeros_like(a.objrefs[i, j])
  result = DistArray()
  result.construct(a.shape, objrefs)
  return result

@op.distributed([DistArray], [DistArray])
def tril(a):
  if a.ndim != 2:
    raise Exception("Input must have 2 dimensions, but a.ndim is " + str(a.ndim))
  objrefs = np.empty(a.num_blocks, dtype=object)
  for i in range(a.num_blocks[0]):
    for j in range(a.num_blocks[1]):
      if i > j:
        objrefs[i, j] = single.copy(a.objrefs[i, j])
      elif i == j:
        objrefs[i, j] = single.tril(a.objrefs[i, j])
      else:
        objrefs[i, j] = single.zeros_like(a.objrefs[i, j])
  result = DistArray()
  result.construct(a.shape, objrefs)
  return result

@op.distributed([np.ndarray, None], [np.ndarray])
def blockwise_dot(*matrices):
  n = len(matrices)
  if n % 2 != 0:
    raise Exception("blockwise_dot expects an even number of arguments, but len(matrices) is {}.".format(n))
  shape = (matrices[0].shape[0], matrices[n / 2].shape[1])
  result = np.zeros(shape)
  for i in range(n / 2):
    result += np.dot(matrices[i], matrices[n / 2 + i])
  return result

@op.distributed([DistArray, DistArray], [DistArray])
def dot(a, b):
  if a.ndim != 2:
    raise Exception("dot expects its arguments to be 2-dimensional, but a.ndim = {}.".format(a.ndim))
  if b.ndim != 2:
    raise Exception("dot expects its arguments to be 2-dimensional, but b.ndim = {}.".format(b.ndim))
  if a.shape[1] != b.shape[0]:
    raise Exception("dot expects a.shape[1] to equal b.shape[0], but a.shape = {} and b.shape = {}.".format(a.shape, b.shape))
  shape = [a.shape[0], b.shape[1]]
  num_blocks = DistArray.compute_num_blocks(shape)
  objrefs = np.empty(num_blocks, dtype=object)
  for i in range(num_blocks[0]):
    for j in range(num_blocks[1]):
      args = list(a.objrefs[i, :]) + list(b.objrefs[:, j])
      objrefs[i, j] = blockwise_dot(*args)
  result = DistArray()
  result.construct(shape, objrefs)
  return result

# This is not in numpy, should we expose this?
@op.distributed([DistArray], [DistArray])
def block_column(a, col):
  if a.ndim != 2:
    raise Exception("block_column expects its argument to be 2-dimensional, but a.ndim = {}, a.shape = {}.".format(a.ndim, a.shape))
  top_block_shape = DistArray.compute_block_shape([0, col])
  shape = [a.shape[0], top_block_shape[1]]
  result = DistArray()
  result.construct(shape, a.objrefs[:, col])
  return result

# This is not in numpy, should we expose this?
@op.distributed([DistArray], [DistArray])
def block_row(a, row):
  if a.ndim != 2:
    raise Exception("block_row expects its argument to be 2-dimensional, but a.ndim = {}, a.shape = {}.".format(a.ndim, a.shape))
  left_block_shape = DistArray.compute_block_shape([row, 0])
  shape = [left_block_shape[0], a.shape[1]]
  result = DistArray()
  result.construct(shape, a.objrefs[row, :])
  return result
