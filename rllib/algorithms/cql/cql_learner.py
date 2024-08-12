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

        # Add a metric to keep track of training iterations to
        # determine when switching the actor loss from behavior
        # cloning to SAC.
        self.metrics.log_value(
            (ALL_MODULES, TRAINING_ITERATION), float("nan"), window=1
        )
