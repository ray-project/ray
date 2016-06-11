from typing import List
import numpy as np
import ray

__all__ = ["matrix_power", "solve", "tensorsolve", "tensorinv", "inv",
           "cholesky", "eigvals", "eigvalsh", "pinv", "slogdet", "det",
           "svd", "eig", "eigh", "lstsq", "norm", "qr", "cond", "matrix_rank",
           "LinAlgError", "multi_dot"]

@ray.remote([np.ndarray, int], [np.ndarray])
def matrix_power(M, n):
  return np.linalg.matrix_power(M, n)

@ray.remote([np.ndarray, np.ndarray], [np.ndarray])
def solve(a, b):
  return np.linalg.solve(a, b)

@ray.remote([np.ndarray], [np.ndarray, np.ndarray])
def tensorsolve(a):
  raise NotImplementedError

@ray.remote([np.ndarray], [np.ndarray, np.ndarray])
def tensorinv(a):
  raise NotImplementedError

@ray.remote([np.ndarray], [np.ndarray])
def inv(a):
  return np.linalg.inv(a)

@ray.remote([np.ndarray], [np.ndarray])
def cholesky(a):
  return np.linalg.cholesky(a)

@ray.remote([np.ndarray], [np.ndarray])
def eigvals(a):
  return np.linalg.eigvals(a)

@ray.remote([np.ndarray], [np.ndarray])
def eigvalsh(a):
  raise NotImplementedError

@ray.remote([np.ndarray], [np.ndarray])
def pinv(a):
  return np.linalg.pinv(a)

@ray.remote([np.ndarray], [int])
def slogdet(a):
  raise NotImplementedError

@ray.remote([np.ndarray], [float])
def det(a):
  return np.linalg.det(a)

@ray.remote([np.ndarray], [np.ndarray, np.ndarray, np.ndarray])
def svd(a):
  return np.linalg.svd(a)

@ray.remote([np.ndarray], [np.ndarray, np.ndarray])
def eig(a):
  return np.linalg.eig(a)

@ray.remote([np.ndarray], [np.ndarray, np.ndarray])
def eigh(a):
  return np.linalg.eigh(a)

@ray.remote([np.ndarray], [np.ndarray, np.ndarray, int, np.ndarray])
def lstsq(a, b):
  return np.linalg.lstsq(a)

@ray.remote([np.ndarray], [float])
def norm(x):
  return np.linalg.norm(x)

@ray.remote([np.ndarray], [np.ndarray, np.ndarray])
def qr(a):
  return np.linalg.qr(a)

@ray.remote([np.ndarray], [float])
def cond(x):
  return np.linalg.cond(x)

@ray.remote([np.ndarray], [int])
def matrix_rank(M):
  return np.linalg.matrix_rank(M)

@ray.remote([np.ndarray], [np.ndarray])
def multi_dot(*a):
  raise NotImplementedError

@ray.remote([np.ndarray], [np.ndarray])
def sum_list(*xs):
  return np.sum(xs, axis=0)
