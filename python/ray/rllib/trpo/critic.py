#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from copy import deepcopy

import numpy as np
import ray
import torch
import torch.nn.functional as F
from torch import Tensor, distributions, nn
from torch.autograd import Variable
from torch.distributions import Normal
from torch.nn import BatchNorm1d, Dropout, Linear, ReLU, Sequential, Softmax


class Critic(nn.Module):
    def __init__(self, obs_shape):
        # TODO use better default network
        # TODO support discrete and cont action spaces
        S, A = obs_shape, action_shape
        H = hidden_size = 512
        R = reward_size = 1

        self.fc1 = Linear(S, H)
        self.fc2 = Linear(H, H)
        self.fc3 = Linear(H, H)
        self.out = Linear(H, R)

    def forward(self, x):
        r = F.relu

        x = r(self.fc1(x))
        x = r(self.fc2(x))
        x = r(self.fc3(x))

        x = self.out(x)

        return x


if __name__ == '__main__':
    pass
