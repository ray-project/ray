from dataclasses import dataclass
from typing import Callable

from ray.rllib.core.learner.differentiable_learner import DifferentiableLearner


@dataclass
class DifferentiableLearnerConfig:

    learner_class: Callable

    lr: float = 3e-5

    # TODO (simon): Add further hps like clip_grad, ...

    num_total_minibatches: int = 0

    num_epochs: int = 1

    minibatch_size: int = None

    shuffle_batch_per_epoch: bool = False

    def __post_init__(self):

        if not issubclass(self.learner_class, DifferentiableLearner):
            raise ValueError(
                "`learner_class` must be a subclass of `DifferentiableLearner "
                f"but is {self.learner_class}."
            )
