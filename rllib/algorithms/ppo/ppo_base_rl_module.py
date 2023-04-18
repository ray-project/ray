"""
This file holds framework-agnostic components for PPO's RLModules.
"""

import abc

from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleConfig
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.core.models.base import ActorCriticEncoder


@ExperimentalAPI
class PPORLModuleBase(RLModule, abc.ABC):
    framework = None

    def __init__(self, config: RLModuleConfig):
        super().__init__(config)

        # __sphinx_doc_begin__
        catalog = self.config.get_catalog()

        # Build models from catalog
        self.encoder = catalog.build_actor_critic_encoder(framework=self.framework)
        self.pi = catalog.build_pi_head(framework=self.framework)
        self.vf = catalog.build_vf_head(framework=self.framework)

        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)
        # __sphinx_doc_end__

        assert isinstance(self.encoder, ActorCriticEncoder)
