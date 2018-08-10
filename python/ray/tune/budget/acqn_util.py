from __future__ import division, print_function
import numpy as np
from scipy.stats import norm


def ei(mu, std, tau):
    '''
    :param mu: mean
    :param std: standard deviation
    :param tau: fixed constant
    :return: ExpectedImprovement
    '''
    z = (tau - mu) / std
    return (tau - mu) * norm.cdf(z) + std * norm.pdf(z)


def longpred(ymu, ys2):
    '''
    long term prediction/rewards
    action-selection function
    :param ymu: mean(s) of all configurations
    :param ys2: variance(s) of all configurations
    :return: the action-value function
    '''
    sidx = np.argsort(ymu)
    bidx = sidx[0]
    y0 = ymu[bidx]
    s0 = ys2[bidx]

    score = ei(ymu, np.sqrt(ys2), y0) - y0
    score[np.isnan(score)] = np.inf
    stmp = ei(y0, np.sqrt(s0), ymu[sidx[1]]) - ymu[sidx[1]]
    stmp = np.nan_to_num(stmp)
    score[bidx] = stmp
    return score


def ttei(ymu, ys2):
    '''
      epsilon-greedy based action-selection function
      :param ymu: mean(s) of all configurations
      :param ys2: variance(s) of all configurations
      :return: the action-value function
      '''
    sidx = np.argsort(ymu)
    bidx = sidx[0]
    y0 = ymu[bidx]
    s0 = ys2[bidx]

    score = ei(ymu, np.sqrt(s0 + ys2), y0)
    score[np.isnan(score)] = np.inf
    score[bidx] = -np.inf
    return score
