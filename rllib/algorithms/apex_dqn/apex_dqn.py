from ray.rllib.algorithms.dqn import DQN, DQNConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/apex_dqn/",
    new="rllib_contrib/apex_dqn/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class ApexDQNConfig(DQNConfig):
    pass


@Deprecated(
    old="rllib/algorithms/apex_dqn/",
    new="rllib_contrib/apex_dqn/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class ApexDQN(DQN):
    pass
