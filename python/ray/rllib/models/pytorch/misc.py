""" Code adapted from https://github.com/ikostrikov/pytorch-a3c"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import torch
from torch.autograd import Variable


def convert_batch(batch, has_features=False):
    """Convert batch from numpy to PT variable"""
    states = Variable(torch.from_numpy(batch.si).float())
    acs = Variable(torch.from_numpy(batch.a))
    advs = Variable(torch.from_numpy(batch.adv.copy()).float())
    advs = advs.view(-1, 1)
    rs = Variable(torch.from_numpy(batch.r.copy()).float())
    rs = rs.view(-1, 1)
    if has_features:
        features = [Variable(torch.from_numpy(f))
                    for f in batch.features]
    else:
        features = batch.features
    return states, acs, advs, rs, features


def normalized_columns_initializer(weights, std=1.0):
    out = torch.randn(weights.size())
    out *= std / torch.sqrt(out.pow(2).sum(1, keepdim=True))
    return out


def var_to_np(var):
    return var.data.numpy()[0]
