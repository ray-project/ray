from typing import Any, Dict, Optional
from dataclasses import dataclass

from ray.ml.checkpoint import Checkpoint


@dataclass
class Result:
    """The final result of a ML training run or a Tune trial.

    This is the class produced by Trainer.fit().
    It contains a checkpoint, which can be used for resuming training and for
    creating a Predictor object. It also contains a metrics object describing
    training metrics. `error` is included so that non successful runs
    and trials can be represented as well.

    The constructor is a private API.

    Args:
        metrics: The final metrics as reported by an Trainable.
        checkpoint: The final checkpoint of the Trainable.
        error: The execution error of the Trainable run, if the trial finishes in error.
    """

    metrics: Optional[Dict[str, Any]]
    checkpoint: Optional[Checkpoint]
    error: Optional[Exception]

    @property
    def config(self) -> Optional[Dict[str, Any]]:
        """The config associated with the result."""
        if not self.metrics:
            return None
        return self.metrics.get("config", None)
