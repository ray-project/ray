from typing import Any, Optional

from ray.ml.checkpoint import Checkpoint


class Result:
    """The final result of a ML training run or a Tune trial.

    This is the class produced by Trainer.fit() or Tuner.fit() (through ResultGrid).
    It contains a checkpoint, which can be used for resuming training and for
    creating a Predictor object. It also contains a metrics object describing
    training metrics. `error` is included so that non successful runs
    and trials can be represented as well.

    The constructor is a private API.

    Args:
        metrics: The final metrics as reported by an Trainable.
        checkpoint: The final checkpoint of the Trainable
        error: The execution error of the Trainable run, if the trial finishes in error.
    """

    def __init__(
        self,
        metrics: Any,
        checkpoint: Optional[Checkpoint],
        error: Optional[Exception] = None,
    ):
        self.metrics = metrics
        self.checkpoint = checkpoint
        self.error = error

    def __getstate__(self) -> dict:
        return self.__dict__

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
