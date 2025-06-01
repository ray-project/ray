"""
This file holds framework-agnostic components for PPO Lagrange's RLModules.
"""
from ray.rllib.algorithms.ppo.ppo_rl_module import PPORLModule
from ray.rllib.utils.annotations import ExperimentalAPI, override


@ExperimentalAPI
class PPOLagrangeRLModule(PPORLModule):
    @override(PPORLModule)
    def setup(self):
        super().setup()
        catalog = self.config.get_catalog()

        # build cost value function from catalog
        self.cvf = catalog.build_cvf_head(framework=self.framework)
