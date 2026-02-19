import logging
from typing import TYPE_CHECKING, Type, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.ppo.ppo import PPO, PPOConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.annotations import override

if TYPE_CHECKING:
    from ray.rllib.core.learner.learner import Learner

logger = logging.getLogger(__name__)


class MAPPO(PPO):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> "MAPPOConfig":
        return MAPPOConfig()


class MAPPOConfig(PPOConfig):
    """Configuration for MAPPO (Multi-Agent PPO with shared centralized critic).

    Extends PPOConfig with a shared critic architecture: decentralized actors
    each have their own policy network, while a single shared value function
    receives concatenated observations from all agents (CTDE paradigm).

    Since the critic is shared and handled separately, per-agent value functions
    are disabled (``use_critic=False``).
    """

    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or MAPPO)
        # MAPPO uses a shared centralized critic instead of per-agent VFs.
        self.use_critic = False

    @override(PPOConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpec:
        from ray.rllib.examples.algorithms.mappo.torch.default_mappo_torch_rl_module import (  # noqa
            DefaultMAPPOTorchRLModule,
        )

        return RLModuleSpec(module_class=DefaultMAPPOTorchRLModule)

    @override(PPOConfig)
    def get_default_learner_class(self) -> Union[Type["Learner"], str]:
        from ray.rllib.examples.algorithms.mappo.torch.mappo_torch_learner import (
            MAPPOTorchLearner,
        )

        return MAPPOTorchLearner
