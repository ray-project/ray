from typing import List
import numpy as np
import ray

__all__ = ["zeros", "zeros_like", "ones", "eye", "dot", "vstack", "hstack", "subarray", "copy", "tril", "triu", "diag", "transpose", "add", "subtract", "sum", "shape"]

@ray.remote([List[int], str, str], [np.ndarray])
def zeros(shape, dtype_name="float", order="C"):
  return np.zeros(shape, dtype=np.dtype(dtype_name), order=order)

@ray.remote([np.ndarray, str, str, bool], [np.ndarray])
def zeros_like(a, dtype_name="None", order="K", subok=True):
  dtype_val = None if dtype_name == "None" else np.dtype(dtype_name)
  return np.zeros_like(a, dtype=dtype_val, order=order, subok=subok)

@ray.remote([List[int], str, str], [np.ndarray])
def ones(shape, dtype_name="float", order="C"):
  return np.ones(shape, dtype=np.dtype(dtype_name), order=order)

@ray.remote([int, int, int, str], [np.ndarray])
def eye(N, M=-1, k=0, dtype_name="float"):
  M = N if M == -1 else M
  return np.eye(N, M=M, k=k, dtype=np.dtype(dtype_name))

@ray.remote([np.ndarray, np.ndarray], [np.ndarray])
def dot(a, b):
  return np.dot(a, b)

@ray.remote([np.ndarray], [np.ndarray])
def vstack(*xs):
  return np.vstack(xs)

@ray.remote([np.ndarray], [np.ndarray])
def hstack(*xs):
  return np.hstack(xs)

# TODO(rkn): instead of this, consider implementing slicing
@ray.remote([np.ndarray, List[int], List[int]], [np.ndarray])
def subarray(a, lower_indices, upper_indices): # TODO(rkn): be consistent about using "index" versus "indices"
  return a[[slice(l, u) for (l, u) in zip(lower_indices, upper_indices)]]

@ray.remote([np.ndarray, str], [np.ndarray])
def copy(a, order="K"):
  return np.copy(a, order=order)

@ray.remote([np.ndarray, int], [np.ndarray])
def tril(m, k=0):
  return np.tril(m, k=k)

@ray.remote([np.ndarray, int], [np.ndarray])
def triu(m, k=0):
  return np.triu(m, k=k)

@ray.remote([np.ndarray, int], [np.ndarray])
def diag(v, k=0):
  return np.diag(v, k=k)

@ray.remote([np.ndarray, List[int]], [np.ndarray])
def transpose(a, axes=[]):
  axes = None if axes == [] else axes
  return np.transpose(a, axes=axes)

@ray.remote([np.ndarray, np.ndarray], [np.ndarray])
def add(x1, x2):
  return np.add(x1, x2)

@ray.remote([np.ndarray, np.ndarray], [np.ndarray])
def subtract(x1, x2):
  return np.subtract(x1, x2)

@ray.remote([int, np.ndarray], [np.ndarray])
def sum(axis, *xs):
  return np.sum(xs, axis=axis)

@ray.remote([np.ndarray], [tuple])
def shape(a):
  return np.shape(a)
