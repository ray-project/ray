from ray.rllib.policy import Policy
from typing import Dict, List
import gym
from gym.spaces import Box, Discrete
import numpy as np

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import SampleBatchType, TensorType, TensorStructType

tf1, tf, tfv = try_import_tf()

class QRegTFModel:
    def __init__(self, policy: Policy, gamma: float, config: Dict) -> None:
        raise NotImplementedError
    
    def reset(self) -> None:
        raise NotImplementedError

    def train_q(self, batch: SampleBatchType) -> None:
        raise NotImplementedError

    def estimate_q(self, batch: SampleBatchType) -> TensorType:
        raise NotImplementedError