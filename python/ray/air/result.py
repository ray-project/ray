from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

from ray.air.checkpoint import Checkpoint
from ray.util.annotations import PublicAPI

import pandas as pd


@dataclass
@PublicAPI(stability="alpha")
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
        metrics_dataframe: The full result dataframe of the Trainable.
            The dataframe is indexed by iterations and contains reported
            metrics.
        best_checkpoints: A list of tuples of the best checkpoints saved
            by the Trainable and their associated metrics. The number of
            saved checkpoints is determined by the ``checkpoint_config``
            argument of ``run_config`` (by default, all checkpoints will
            be saved).
    """

    metrics: Optional[Dict[str, Any]]
    checkpoint: Optional[Checkpoint]
    error: Optional[Exception]
    metrics_dataframe: Optional[pd.DataFrame]
    best_checkpoints: Optional[List[Tuple[Checkpoint, Dict[str, Any]]]]

    @property
    def config(self) -> Optional[Dict[str, Any]]:
        """The config associated with the result."""
        if not self.metrics:
            return None
        return self.metrics.get("config", None)
