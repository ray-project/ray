from typing import Any, Optional

from ray.ml.checkpoint import Checkpoint


class Result:
    """The final result of a ML training run or a Tune trial.

    This is the class produced by Trainer.fit() or Tuner.fit() (through ResultGrid).
    It contains a checkpoint, which can be used for resuming training and for
    creating a Predictor object. It also contains a metrics object describing
    training metrics. `status` and `error` are included so that non successful runs
    and trials can be represented as well.

    Args:
        metrics: The final metrics as reported by an Trainable.
        checkpoint: The final checkpoint of the Trainable.
        status: The status of the Trainable run. Should be between TERMINATED and ERROR.
        error: The execution error of the Trainable run, when `status` is reported as
            ERROR.
    """

    def __init__(
        self,
        metrics: Any,
        checkpoint: Optional[Checkpoint],
        status: str,
        error: Optional[str] = None,
    ):
        self.metrics = metrics
        self.checkpoint = checkpoint
        self.status = status
        self.error = error

    def __getstate__(self) -> dict:
        return self.__dict__

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
