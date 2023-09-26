from dataclasses import dataclass
from ray.rllib.core.learner.learner import LearnerHyperparameters


@dataclass
class SACLearnerHyperparameters(LearnerHyperparameters):
    """Hyperparameters for the SACLearner sub-classes (framework-specific)."""

    # TODO (simon): Set to 'True' as soon as implemented.
    twin_q: bool = False

    # For debugging purposes.
    _deterministic_loss: bool = False
