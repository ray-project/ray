from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from numbers import Number

import numpy as np
import tensorflow as tf

from gym.spaces import Box

import ray
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.utils.annotations import override

from .models import GaussianLatentSpacePolicy, feedforward_model


class SACPolicyGraph(TFPolicyGraph):
    pass
