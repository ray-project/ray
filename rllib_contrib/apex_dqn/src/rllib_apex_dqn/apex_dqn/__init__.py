from rllib_apex_dqn.apex_dqn.apex_dqn import ApexDQN, ApexDQNConfig

from ray.tune.registry import register_trainable

__all__ = ["ApexDQNConfig", "ApexDQN"]

register_trainable("rllib-contrib-apex-dqn", ApexDQN)
