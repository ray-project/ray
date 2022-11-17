import ray.rllib.agents.dqn.apex as apex  # noqa
import ray.rllib.agents.dqn.simple_q as simple_q  # noqa
from ray.rllib.algorithms.apex_dqn.apex_dqn import APEX_DEFAULT_CONFIG
from ray.rllib.algorithms.apex_dqn.apex_dqn import ApexDQN as ApexTrainer
from ray.rllib.algorithms.apex_dqn.apex_dqn import ApexDQNConfig
from ray.rllib.algorithms.dqn.dqn import DEFAULT_CONFIG
from ray.rllib.algorithms.dqn.dqn import DQN as DQNTrainer
from ray.rllib.algorithms.dqn.dqn import DQNConfig
from ray.rllib.algorithms.dqn.dqn_tf_policy import DQNTFPolicy
from ray.rllib.algorithms.dqn.dqn_torch_policy import DQNTorchPolicy
from ray.rllib.algorithms.r2d2.r2d2 import R2D2 as R2D2Trainer
from ray.rllib.algorithms.r2d2.r2d2 import R2D2_DEFAULT_CONFIG, R2D2Config
from ray.rllib.algorithms.r2d2.r2d2_tf_policy import R2D2TFPolicy
from ray.rllib.algorithms.r2d2.r2d2_torch_policy import R2D2TorchPolicy
from ray.rllib.algorithms.simple_q.simple_q import (
    DEFAULT_CONFIG as SIMPLE_Q_DEFAULT_CONFIG,
)
from ray.rllib.algorithms.simple_q.simple_q import SimpleQ as SimpleQTrainer
from ray.rllib.algorithms.simple_q.simple_q import SimpleQConfig
from ray.rllib.algorithms.simple_q.simple_q_tf_policy import (
    SimpleQTF1Policy,
    SimpleQTF2Policy,
)
from ray.rllib.algorithms.simple_q.simple_q_torch_policy import SimpleQTorchPolicy
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "ApexDQNConfig",
    "ApexTrainer",
    "DQNConfig",
    "DQNTFPolicy",
    "DQNTorchPolicy",
    "DQNTrainer",
    "R2D2Config",
    "R2D2TFPolicy",
    "R2D2TorchPolicy",
    "R2D2Trainer",
    "SimpleQConfig",
    "SimpleQTF1Policy",
    "SimpleQTF2Policy",
    "SimpleQTorchPolicy",
    "SimpleQTrainer",
    # Deprecated.
    "APEX_DEFAULT_CONFIG",
    "DEFAULT_CONFIG",
    "R2D2_DEFAULT_CONFIG",
    "SIMPLE_Q_DEFAULT_CONFIG",
]


deprecation_warning(
    "ray.rllib.agents.dqn",
    "ray.rllib.algorithms.[dqn|simple_q|r2d2|apex_dqn]",
    error=True,
)
