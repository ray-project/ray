from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray


@ray.task
def zeros(shape, dtype_name="float", order="C"):
  return np.zeros(shape, dtype=np.dtype(dtype_name), order=order)


@ray.task
def zeros_like(a, dtype_name="None", order="K", subok=True):
  dtype_val = None if dtype_name == "None" else np.dtype(dtype_name)
  return np.zeros_like(a, dtype=dtype_val, order=order, subok=subok)


@ray.task
def ones(shape, dtype_name="float", order="C"):
  return np.ones(shape, dtype=np.dtype(dtype_name), order=order)


@ray.task
def eye(N, M=-1, k=0, dtype_name="float"):
  M = N if M == -1 else M
  return np.eye(N, M=M, k=k, dtype=np.dtype(dtype_name))


@ray.task
def dot(a, b):
  return np.dot(a, b)


@ray.task
def vstack(*xs):
  return np.vstack(xs)


@ray.task
def hstack(*xs):
  return np.hstack(xs)


# TODO(rkn): Instead of this, consider implementing slicing.
# TODO(rkn): Be consistent about using "index" versus "indices".
@ray.task
def subarray(a, lower_indices, upper_indices):
  return a[[slice(l, u) for (l, u) in zip(lower_indices, upper_indices)]]


@ray.task
def copy(a, order="K"):
  return np.copy(a, order=order)


@ray.task
def tril(m, k=0):
  return np.tril(m, k=k)


@ray.task
def triu(m, k=0):
  return np.triu(m, k=k)


@ray.task
def diag(v, k=0):
  return np.diag(v, k=k)


@ray.task
def transpose(a, axes=[]):
  axes = None if axes == [] else axes
  return np.transpose(a, axes=axes)


@ray.task
def add(x1, x2):
  return np.add(x1, x2)


@ray.task
def subtract(x1, x2):
  return np.subtract(x1, x2)


@ray.task
def sum(x, axis=-1):
  return np.sum(x, axis=axis if axis != -1 else None)


@ray.task
def shape(a):
  return np.shape(a)


@ray.task
def sum_list(*xs):
  return np.sum(xs, axis=0)
