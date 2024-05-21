"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override


class DreamerV3Learner(Learner):
    """DreamerV3 specific Learner class.

    Only implements the `additional_update_for_module()` method to define the logic
    for updating the critic EMA-copy after each training step.
    """

    @override(Learner)
    def _after_update(self, timesteps):

        super()._after_update(timesteps)

        # Update EMA weights of the critic.
        for module_id, module in self.module._rl_modules.items():
            module.critic.update_ema()
