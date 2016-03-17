from typing import List
import numpy as np
import orchpy as op

@op.distributed([List[int]], [np.ndarray])
def zeros(shape):
  return np.zeros(shape)

@op.distributed([List[int]], [np.ndarray])
def ones(shape):
  return np.ones(shape)

@op.distributed([int], [np.ndarray])
def eye(dim):
  return np.eye(dim)

@op.distributed([np.ndarray, np.ndarray], [np.ndarray])
def dot(a, b):
  return np.dot(a, b)

# TODO(rkn): My preferred signature would have been
# @op.distributed([List[np.ndarray]], [np.ndarray]) but that currently doesn't
# work because that would expect a list of ndarrays not a list of ObjRefs
@op.distributed([np.ndarray, None], [np.ndarray])
def vstack(*xs):
  return np.vstack(xs)

@op.distributed([np.ndarray, None], [np.ndarray])
def hstack(*xs):
  return np.hstack(xs)

# TODO(rkn): this doesn't parallel the numpy API, but we can't really slice an ObjRef, think about this
@op.distributed([np.ndarray, List[int], List[int]], [np.ndarray])
def subarray(a, lower_indices, upper_indices): # TODO(rkn): be consistent about using "index" versus "indices"
  return a[[slice(l, u) for (l, u) in zip(lower_indices, upper_indices)]]

@op.distributed([np.ndarray], [np.ndarray])
def copy(a):
  return np.copy(a)

@op.distributed([np.ndarray], [np.ndarray])
def tril(a):
  return np.tril(a)

@op.distributed([np.ndarray], [np.ndarray])
def triu(a):
  return np.triu(a)
