from dataclasses import dataclass
from typing import Callable

from ray.rllib.core.learner.differentiable_learner import DifferentiableLearner


@dataclass
class DifferentiableLearnerConfig:
    """Configures a `DifferentiableLearner`."""

    # TODO (simon): We implement only for `PyTorch`, so maybe we use here directly
    # TorchDifferentiableLearner` and check for this?
    # The `DifferentiableLearner` class. Must be derived from `DifferentiableLearner`.
    learner_class: Callable

    # The learning rate to use for the nested update. Note, in the default case this
    # learning rate is only used to update parameters in a functional form, i.e. the
    # `RLModule`'s stateful parameters are only updated in the `MetaLearner`. Different
    # logic can be implemented in customized `DifferentiableLearner`s.
    lr: float = 3e-5

    # TODO (simon): Add further hps like clip_grad, ...
    # The total number of minibatches to be formed from the batch per learner, e.g.
    # setting `train_batch_size_per_learner=10` and `num_total_minibatches` to 2
    # runs 2 SGD minibatch updates with a batch of 5 per training iteration.
    num_total_minibatches: int = 0

    # The number of epochs per training iteration.
    num_epochs: int = 1

    # The minibatch size per SGD minibatch update, e.g. with a `train_batch_size_per_learner=10`
    # and a `minibatch_size=2` the training step runs 5 SGD minibatch updates with minibatches
    # of 2.
    minibatch_size: int = None

    # If the batch should be shuffled between epochs.
    shuffle_batch_per_epoch: bool = False

    def __post_init__(self):
        """Additional initialization processes."""

        # Ensure we have a `DifferentiableLearner` class.
        if not issubclass(self.learner_class, DifferentiableLearner):
            raise ValueError(
                "`learner_class` must be a subclass of `DifferentiableLearner "
                f"but is {self.learner_class}."
            )
