from typing import TYPE_CHECKING
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ray.air.checkpoint import Checkpoint
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pandas as pd


@dataclass
@PublicAPI(stability="beta")
class Result:
    """The final result of a ML training run or a Tune trial.

    This is the class produced by Trainer.fit().
    It contains a checkpoint, which can be used for resuming training and for
    creating a Predictor object. It also contains a metrics object describing
    training metrics. ``error`` is included so that non successful runs
    and trials can be represented as well.

    The constructor is a private API.

    Args:
        metrics: The final metrics as reported by an Trainable.
        checkpoint: The final checkpoint of the Trainable.
        error: The execution error of the Trainable run, if the trial finishes in error.
        log_dir: Directory where the trial logs are saved.
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
    log_dir: Optional[Path]
    metrics_dataframe: Optional["pd.DataFrame"]
    best_checkpoints: Optional[List[Tuple[Checkpoint, Dict[str, Any]]]]
    _items_to_repr = ["metrics", "error", "log_dir"]

    @property
    def config(self) -> Optional[Dict[str, Any]]:
        """The config associated with the result."""
        if not self.metrics:
            return None
        return self.metrics.get("config", None)

    @property
    def uri(self) -> Optional[str]:
        """Return checkpoint URI, if available.

        Same as ``self.checkpoint.uri``."""
        if not self.checkpoint:
            return None
        return self.checkpoint.uri

    def __repr__(self):
        from ray.tune.result import AUTO_RESULT_KEYS

        shown_attributes = {k: self.__dict__[k] for k in self._items_to_repr}

        if self.metrics:
            shown_attributes["metrics"] = {
                k: v for k, v in self.metrics.items() if k not in AUTO_RESULT_KEYS
            }
        kws = [f"{key}={value!r}" for key, value in shown_attributes.items()]
        return "{}({})".format(type(self).__name__, ", ".join(kws))
