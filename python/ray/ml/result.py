from typing import Any, Optional

from ray.ml.checkpoint import Checkpoint


class Result:
    """The final result of a ML training run.

    This is the class produced by Trainer.fit(). It contains a checkpoint, which
    can be used for resuming training and for creating a Predictor object. It also
    contains a metrics object describing training metrics.
    """

    checkpoint: Optional[Checkpoint]
    metrics: Any

    def __getstate__(self) -> dict:
        return self.__dict__

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
