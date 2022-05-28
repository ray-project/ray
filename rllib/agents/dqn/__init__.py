from ray.rllib.agents.dqn.apex import ApexConfig, ApexTrainer, APEX_DEFAULT_CONFIG
from ray.rllib.algorithms.dqn.dqn import DQNConfig, DQNTrainer, DEFAULT_CONFIG
from ray.rllib.algorithms.dqn.dqn_tf_policy import DQNTFPolicy
from ray.rllib.algorithms.dqn.dqn_torch_policy import DQNTorchPolicy
from ray.rllib.agents.dqn.r2d2 import R2D2Trainer, R2D2_DEFAULT_CONFIG
from ray.rllib.agents.dqn.r2d2_torch_policy import R2D2TorchPolicy
from ray.rllib.algorithms.dqn.simple_q import (
    SimpleQConfig,
    SimpleQTrainer,
    DEFAULT_CONFIG as SIMPLE_Q_DEFAULT_CONFIG,
)
from ray.rllib.algorithms.dqn.simple_q_tf_policy import SimpleQTFPolicy
from ray.rllib.algorithms.dqn.simple_q_torch_policy import SimpleQTorchPolicy

__all__ = [
    "ApexConfig",
    "ApexTrainer",
    "DQNConfig",
    "DQNTFPolicy",
    "DQNTorchPolicy",
    "DQNTrainer",
    "R2D2TorchPolicy",
    "R2D2Trainer",
    "SimpleQConfig",
    "SimpleQTFPolicy",
    "SimpleQTorchPolicy",
    "SimpleQTrainer",
    # Deprecated.
    "APEX_DEFAULT_CONFIG",
    "DEFAULT_CONFIG",
    "R2D2_DEFAULT_CONFIG",
    "SIMPLE_Q_DEFAULT_CONFIG",
]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("ray.rllib.agents.dqn", "ray.rllib.algorithms.dqn", error=False)
