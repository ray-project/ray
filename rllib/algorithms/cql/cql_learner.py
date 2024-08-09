from ray.rllib.algorithms.sac.sac_learner import SACLearner

from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override


class CQLLearner(SACLearner):
    @override(Learner)
    def build(self) -> None:

        # Set up the gradient buffer to store gradients to apply
        # them later in `self.apply_gradients`.
        self.grads = {}

        # We need to call the `super()`'s `build` method here to have the variables
        # for `alpha`` and the target entropy defined.
        super().build()
