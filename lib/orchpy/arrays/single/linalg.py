from typing import List
import numpy as np
import orchpy as op

__all__ = ["matrix_power", "solve", "tensorsolve", "tensorinv", "inv",
           "cholesky", "eigvals", "eigvalsh", "pinv", "slogdet", "det",
           "svd", "eig", "eigh", "lstsq", "norm", "qr", "cond", "matrix_rank",
           "LinAlgError", "multi_dot"]

@op.distributed([np.ndarray, int], [np.ndarray])
def matrix_power(M, n):
  return np.linalg.matrix_power(M, n)

@op.distributed([np.ndarray, np.ndarray], [np.ndarray])
def solve(a, b):
  return np.linalg.solve(a, b)

@op.distributed([np.ndarray], [np.ndarray, np.ndarray])
def tensorsolve(a):
  raise NotImplementedError

@op.distributed([np.ndarray], [np.ndarray, np.ndarray])
def tensorinv(a):
  raise NotImplementedError

@op.distributed([np.ndarray], [np.ndarray])
def inv(a):
  return np.linalg.inv(a)

@op.distributed([np.ndarray], [np.ndarray])
def cholesky(a):
  return np.linalg.cholesky(a)

@op.distributed([np.ndarray], [np.ndarray])
def eigvals(a):
  return np.linalg.eigvals(a)

@op.distributed([np.ndarray], [np.ndarray])
def eigvalsh(a):
  raise NotImplementedError

@op.distributed([np.ndarray], [np.ndarray])
def pinv(a):
  return np.linalg.pinv(a)

@op.distributed([np.ndarray], [int])
def slogdet(a):
  raise NotImplementedError

@op.distributed([np.ndarray], [float])
def det(a):
  return np.linalg.det(a)

@op.distributed([np.ndarray], [np.ndarray, np.ndarray, np.ndarray])
def svd(a):
  return np.linalg.svd(a)

@op.distributed([np.ndarray], [np.ndarray, np.ndarray])
def eig(a):
  return np.linalg.eig(a)

@op.distributed([np.ndarray], [np.ndarray, np.ndarray])
def eigh(a):
  return np.linalg.eigh(a)

@op.distributed([np.ndarray], [np.ndarray, np.ndarray, int, np.ndarray])
def lstsq(a, b):
  return np.linalg.lstsq(a)

@op.distributed([np.ndarray], [float])
def norm(x):
  return np.linalg.norm(x)

@op.distributed([np.ndarray], [np.ndarray, np.ndarray])
def qr(a):
  return np.linalg.qr(a)

@op.distributed([np.ndarray], [float])
def cond(x):
  return np.linalg.cond(x)

@op.distributed([np.ndarray], [int])
def matrix_rank(M):
  return np.linalg.matrix_rank(M)

@op.distributed([np.ndarray, None], [np.ndarray])
def multi_dot(a):
  raise NotImplementedError



# This isn't in numpy, should we expose it?
@op.distributed([np.ndarray], [np.ndarray, np.ndarray, np.ndarray])
def modified_lu(q):
  """
  Algorithm 5 from http://www.eecs.berkeley.edu/Pubs/TechRpts/2013/EECS-2013-175.pdf
  takes a matrix q with orthonormal columns, returns l, u, s such that q - s = l * u
  arguments:
    q: a two dimensional orthonormal q
  return values:
    l: lower triangular
    u: upper triangular
    s: a diagonal matrix represented by its diagonal
  """
  m, b = q.shape[0], q.shape[1]
  S = np.zeros(b)

  q_work = np.copy(q)

  for i in range(b):
    S[i] = -1 * np.sign(q_work[i, i])
    q_work[i, i] -= S[i]

    # scale ith column of L by diagonal element
    q_work[(i + 1):m, i] /= q_work[i, i]

    # perform Schur complement update
    q_work[(i + 1):m, (i + 1):b] -= np.outer(q_work[(i + 1):m, i], q_work[i, (i + 1):b])

  L = np.tril(q_work)
  for i in range(b):
    L[i, i] = 1
  U = np.triu(q_work)[:b, :]
  return L, U, S
