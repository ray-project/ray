from typing import List
import numpy as np
import orchpy as op

__all__ = ["zeros", "zeros_like", "ones", "eye", "dot", "vstack", "hstack", "subarray", "copy", "tril", "triu", "diag", "transpose", "add", "subtract", "eye2", "sum", "shape"]

@op.distributed([List[int], str], [np.ndarray])
def zeros(shape, dtype_name):
  return np.zeros(shape, dtype=np.dtype(dtype_name))

@op.distributed([np.ndarray], [np.ndarray])
def zeros_like(x):
  return np.zeros_like(x)

@op.distributed([List[int], str], [np.ndarray])
def ones(shape, dtype_name):
  return np.ones(shape, dtype=np.dtype(dtype_name))

@op.distributed([int, str], [np.ndarray])
def eye(dim, dtype_name):
  return np.eye(dim, dtype=np.dtype(dtype_name))

# TODO(rkn): This should be part of eye
@op.distributed([int, int, str], [np.ndarray])
def eye2(dim1, dim2, dtype_name):
  return np.eye(dim1, dim2, dtype=np.dtype(dtype_name))

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

@op.distributed([np.ndarray], [np.ndarray])
def diag(a):
  return np.diag(a)

@op.distributed([np.ndarray], [np.ndarray])
def transpose(a):
  return np.transpose(a)

@op.distributed([np.ndarray, np.ndarray], [np.ndarray])
def add(x1, x2):
  return np.add(x1, x2)

@op.distributed([np.ndarray, np.ndarray], [np.ndarray])
def subtract(x1, x2):
  return np.subtract(x1, x2)

@op.distributed([int, np.ndarray, None], [np.ndarray])
def sum(axis, *xs):
  return np.sum(xs, axis=axis)

@op.distributed([np.ndarray], [tuple])
def shape(a):
  return np.shape(a)
