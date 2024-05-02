"""
This file holds framework-agnostic components for APPO's RLModules.
"""

import abc

from ray.rllib.algorithms.ppo.ppo_rl_module import PPORLModule
from ray.rllib.utils.annotations import ExperimentalAPI

# TODO (simon): Write a light-weight version of this class for the `TFRLModule`


@ExperimentalAPI
class APPORLModule(PPORLModule, abc.ABC):
    def setup(self):
        super().setup()

        # If the module is not for inference only, set up the target networks.
        if not self.inference_only:
            catalog = self.config.get_catalog()
            # Old pi and old encoder are the "target networks" that are used for
            # the stabilization of the updates of the current pi and encoder.
            self.old_pi = catalog.build_pi_head(framework=self.framework)
            self.old_encoder = catalog.build_actor_critic_encoder(
                framework=self.framework
            )
            self.old_pi.load_state_dict(self.pi.state_dict())
            self.old_encoder.load_state_dict(self.encoder.state_dict())
            # We do not train the targets.
            self.old_pi.requires_grad_(False)
            self.old_encoder.requires_grad_(False)
