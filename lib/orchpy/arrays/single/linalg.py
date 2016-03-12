from typing import List
import numpy as np
import orchpy as op

# TODO(rkn): this should take the same optional "mode" argument as np.linalg.qr, except that the different options sometimes have different numbers of return values, which could be a problem
@op.distributed([np.ndarray], [np.ndarray, np.ndarray])
def qr(a):
  """
  Suppose (n, m) = a.shape
  If n >= m:
    q.shape == (n, m)
    r.shape == (m, m)
  If n < m:
    q.shape == (n, n)
    r.shape == (n, m)
  """
  return np.linalg.qr(a)

#@op.distributed([np.ndarray], [np.ndarray, np.ndarray, np.ndarray])
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
