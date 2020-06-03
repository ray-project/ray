from collections import OrderedDict
import logging
import gym

from ray.rllib.models.tf.layers import GRUGate, RelativeMultiHeadAttention, \
    SkipConnection

tf = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


if name == "__main__":
    print("Ran")