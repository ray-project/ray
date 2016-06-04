from typing import List

import numpy as np
import arrays.single as single
import halo

from core import *

__all__ = ["tsqr", "modified_lu", "tsqr_hr", "qr"]

@halo.distributed([DistArray], [DistArray, np.ndarray])
def tsqr(a):
  """
  arguments:
    a: a distributed matrix
  Suppose that
    a.shape == (M, N)
    K == min(M, N)
  return values:
    q: DistArray, if q_full = halo.context.pull(DistArray, q).assemble(), then
      q_full.shape == (M, K)
      np.allclose(np.dot(q_full.T, q_full), np.eye(K)) == True
    r: np.ndarray, if r_val = halo.context.pull(np.ndarray, r), then
      r_val.shape == (K, N)
      np.allclose(r, np.triu(r)) == True
  """
  if len(a.shape) != 2:
    raise Exception("tsqr requires len(a.shape) == 2, but a.shape is {}".format(a.shape))
  if a.num_blocks[1] != 1:
    raise Exception("tsqr requires a.num_blocks[1] == 1, but a.num_blocks is {}".format(a.num_blocks))

  num_blocks = a.num_blocks[0]
  K = int(np.ceil(np.log2(num_blocks))) + 1
  q_tree = np.empty((num_blocks, K), dtype=object)
  current_rs = []
  for i in range(num_blocks):
    block = a.objrefs[i, 0]
    q, r = single.linalg.qr(block)
    q_tree[i, 0] = q
    current_rs.append(r)
  for j in range(1, K):
    new_rs = []
    for i in range(int(np.ceil(1.0 * len(current_rs) / 2))):
      stacked_rs = single.vstack(*current_rs[(2 * i):(2 * i + 2)])
      q, r = single.linalg.qr(stacked_rs)
      q_tree[i, j] = q
      new_rs.append(r)
    current_rs = new_rs
  assert len(current_rs) == 1, "len(current_rs) = " + str(len(current_rs))

  q_result = DistArray()

  # handle the special case in which the whole DistArray "a" fits in one block
  # and has fewer rows than columns, this is a bit ugly so think about how to
  # remove it
  if a.shape[0] >= a.shape[1]:
    q_shape = a.shape
  else:
    q_shape = [a.shape[0], a.shape[0]]
  q_num_blocks = DistArray.compute_num_blocks(q_shape)
  q_result = DistArray()
  q_objrefs = np.empty(q_num_blocks, dtype=object)
  q_result.construct(q_shape, q_objrefs)

  # reconstruct output
  for i in range(num_blocks):
    q_block_current = q_tree[i, 0]
    ith_index = i
    for j in range(1, K):
      if np.mod(ith_index, 2) == 0:
        lower = [0, 0]
        upper = [a.shape[1], BLOCK_SIZE]
      else:
        lower = [a.shape[1], 0]
        upper = [2 * a.shape[1], BLOCK_SIZE]
      ith_index /= 2
      q_block_current = single.dot(q_block_current, single.subarray(q_tree[ith_index, j], lower, upper))
    q_result.objrefs[i] = q_block_current
  r = current_rs[0]
  return q_result, r

# TODO(rkn): This is unoptimized, we really want a block version of this.
@halo.distributed([DistArray], [DistArray, np.ndarray, np.ndarray])
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
  q = q.assemble()
  m, b = q.shape[0], q.shape[1]
  S = np.zeros(b)

  q_work = np.copy(q)

  for i in range(b):
    S[i] = -1 * np.sign(q_work[i, i])
    q_work[i, i] -= S[i]
    q_work[(i + 1):m, i] /= q_work[i, i] # scale ith column of L by diagonal element
    q_work[(i + 1):m, (i + 1):b] -= np.outer(q_work[(i + 1):m, i], q_work[i, (i + 1):b]) # perform Schur complement update

  L = np.tril(q_work)
  for i in range(b):
    L[i, i] = 1
  U = np.triu(q_work)[:b, :]
  return numpy_to_dist(halo.push(L)), U, S # TODO(rkn): get rid of push and pull

@halo.distributed([np.ndarray, np.ndarray, np.ndarray, int], [np.ndarray, np.ndarray])
def tsqr_hr_helper1(u, s, y_top_block, b):
  y_top = y_top_block[:b, :b]
  s_full = np.diag(s)
  t = -1 * np.dot(u, np.dot(s_full, np.linalg.inv(y_top).T))
  return t, y_top

@halo.distributed([np.ndarray, np.ndarray], [np.ndarray])
def tsqr_hr_helper2(s, r_temp):
  s_full = np.diag(s)
  return np.dot(s_full, r_temp)

@halo.distributed([DistArray], [DistArray, np.ndarray, np.ndarray, np.ndarray])
def tsqr_hr(a):
  """Algorithm 6 from http://www.eecs.berkeley.edu/Pubs/TechRpts/2013/EECS-2013-175.pdf"""
  q, r_temp = tsqr(a)
  y, u, s = modified_lu(q)
  y_blocked = halo.pull(y)
  t, y_top = tsqr_hr_helper1(u, s, y_blocked.objrefs[0, 0], a.shape[1])
  r = tsqr_hr_helper2(s, r_temp)
  return y, t, y_top, r

@halo.distributed([np.ndarray, np.ndarray, np.ndarray, np.ndarray], [np.ndarray])
def qr_helper1(a_rc, y_ri, t, W_c):
  return a_rc - np.dot(y_ri, np.dot(t.T, W_c))

@halo.distributed([np.ndarray, np.ndarray], [np.ndarray])
def qr_helper2(y_ri, a_rc):
  return np.dot(y_ri.T, a_rc)

@halo.distributed([DistArray], [DistArray, DistArray])
def qr(a):
  """Algorithm 7 from http://www.eecs.berkeley.edu/Pubs/TechRpts/2013/EECS-2013-175.pdf"""
  m, n = a.shape[0], a.shape[1]
  k = min(m, n)

  # we will store our scratch work in a_work
  a_work = DistArray()
  a_work.construct(a.shape, np.copy(a.objrefs))

  result_dtype = np.linalg.qr(halo.pull(a.objrefs[0, 0]))[0].dtype.name
  r_res = halo.pull(zeros([k, n], result_dtype)) # TODO(rkn): It would be preferable not to pull this right after creating it.
  y_res = halo.pull(zeros([m, k], result_dtype)) # TODO(rkn): It would be preferable not to pull this right after creating it.
  Ts = []

  for i in range(min(a.num_blocks[0], a.num_blocks[1])): # this differs from the paper, which says "for i in range(a.num_blocks[1])", but that doesn't seem to make any sense when a.num_blocks[1] > a.num_blocks[0]
    sub_dist_array = subblocks(a_work, range(i, a_work.num_blocks[0]), [i])
    y, t, _, R = tsqr_hr(sub_dist_array)
    y_val = halo.pull(y)

    for j in range(i, a.num_blocks[0]):
      y_res.objrefs[j, i] = y_val.objrefs[j - i, 0]
    if a.shape[0] > a.shape[1]:
      # in this case, R needs to be square
      R_shape = halo.pull(single.shape(R))
      eye_temp = single.eye(R_shape[1], R_shape[0], dtype_name=result_dtype)
      r_res.objrefs[i, i] = single.dot(eye_temp, R)
    else:
      r_res.objrefs[i, i] = R
    Ts.append(numpy_to_dist(t))

    for c in range(i + 1, a.num_blocks[1]):
      W_rcs = []
      for r in range(i, a.num_blocks[0]):
        y_ri = y_val.objrefs[r - i, 0]
        W_rcs.append(qr_helper2(y_ri, a_work.objrefs[r, c]))
      W_c = single.sum(0, *W_rcs)
      for r in range(i, a.num_blocks[0]):
        y_ri = y_val.objrefs[r - i, 0]
        A_rc = qr_helper1(a_work.objrefs[r, c], y_ri, t, W_c)
        a_work.objrefs[r, c] = A_rc
      r_res.objrefs[i, c] = a_work.objrefs[i, c]

  # construct q_res from Ys and Ts
  q = eye(m, k, dtype_name=result_dtype)
  for i in range(len(Ts))[::-1]:
    y_col_block = subblocks(y_res, [], [i])
    q = subtract(q, dot(y_col_block, dot(Ts[i], dot(transpose(y_col_block), q))))

  return q, r_res
