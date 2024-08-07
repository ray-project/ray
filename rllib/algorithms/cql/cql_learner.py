from ray.air.constants import TRAINING_ITERATION
from ray.rllib.algorithms.sac.sac_learner import SACLearner
from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import ALL_MODULES


class CQLLearner(SACLearner):
    @override(Learner)
    def build(self) -> None:
        # We need to call the `super()`'s `build` method here to have the variables
        # for `alpha`` and the target entropy defined.
        super().build()

        # Set up the gradient buffer to store gradients to apply
        # them later in `self.apply_gradients`.
        self.grads = {}

        # Log the training iteration to switch from behavior cloning to improving
        # the policy.
        # TODO (simon, sven): Add upstream information pieces into this timesteps
        # call arg to Learner.update_...().
        self.metrics.log_value(
            (ALL_MODULES, TRAINING_ITERATION), float("nan"), window=1
        )
