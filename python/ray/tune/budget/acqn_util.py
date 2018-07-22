from __future__ import division, print_function
import numpy as np
from scipy.stats import norm

def ei(mu, std, tau):
  z = (tau - mu) / std
  return (tau - mu) * norm.cdf(z) + std * norm.pdf(z)


def longreg(ymu, ys2, method):
  sidx = np.argsort(ymu)
  bidx = sidx[0]
  y0 = ymu[bidx]
  s0 = ys2[bidx]

  if method is 'th1':
    score = ei(ymu, np.sqrt(ys2), y0) - y0
    score[np.isnan(score)] = np.inf
    stmp = ei(y0, np.sqrt(s0), ymu[sidx[1]]) - ymu[sidx[1]]
    stmp = np.nan_to_num(stmp)
    score[bidx] = stmp
  elif method is 'old':
    score = ei(ymu, np.sqrt(s0 + ys2), y0)
  return score

def ttei(ymu, ys2):
  sidx = np.argsort(ymu)
  bidx = sidx[0]
  y0 = ymu[bidx]
  s0 = ys2[bidx]

  score = ei(ymu, np.sqrt(s0 + ys2), y0)
  score[np.isnan(score)] = np.inf
  score[bidx] = -np.inf
  return score