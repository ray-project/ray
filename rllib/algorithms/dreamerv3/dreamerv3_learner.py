"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    override,
)


class DreamerV3Learner(Learner):
    """DreamerV3 specific Learner class.

    Only implements the `after_gradient_based_update()` method to define the logic
    for updating the critic EMA-copy after each training step.
    """

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(Learner)
    def after_gradient_based_update(self, *, timesteps):
        super().after_gradient_based_update(timesteps=timesteps)

        # Update EMA weights of the critic.
        for module_id, module in self.module._rl_modules.items():
            module.unwrapped().critic.update_ema()
