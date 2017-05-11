from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray

__all__ = ["matrix_power", "solve", "tensorsolve", "tensorinv", "inv",
           "cholesky", "eigvals", "eigvalsh", "pinv", "slogdet", "det",
           "svd", "eig", "eigh", "lstsq", "norm", "qr", "cond", "matrix_rank",
           "multi_dot"]


@ray.task
def matrix_power(M, n):
  return np.linalg.matrix_power(M, n)


@ray.task
def solve(a, b):
  return np.linalg.solve(a, b)


@ray.task(num_return_vals=2)
def tensorsolve(a):
  raise NotImplementedError


@ray.task(num_return_vals=2)
def tensorinv(a):
  raise NotImplementedError


@ray.task
def inv(a):
  return np.linalg.inv(a)


@ray.task
def cholesky(a):
  return np.linalg.cholesky(a)


@ray.task
def eigvals(a):
  return np.linalg.eigvals(a)


@ray.task
def eigvalsh(a):
  raise NotImplementedError


@ray.task
def pinv(a):
  return np.linalg.pinv(a)


@ray.task
def slogdet(a):
  raise NotImplementedError


@ray.task
def det(a):
  return np.linalg.det(a)


@ray.task(num_return_vals=3)
def svd(a):
  return np.linalg.svd(a)


@ray.task(num_return_vals=2)
def eig(a):
  return np.linalg.eig(a)


@ray.task(num_return_vals=2)
def eigh(a):
  return np.linalg.eigh(a)


@ray.task(num_return_vals=4)
def lstsq(a, b):
  return np.linalg.lstsq(a)


@ray.task
def norm(x):
  return np.linalg.norm(x)


@ray.task(num_return_vals=2)
def qr(a):
  return np.linalg.qr(a)


@ray.task
def cond(x):
  return np.linalg.cond(x)


@ray.task
def matrix_rank(M):
  return np.linalg.matrix_rank(M)


@ray.task
def multi_dot(*a):
  raise NotImplementedError
