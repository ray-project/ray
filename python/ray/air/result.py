import warnings
from typing import TYPE_CHECKING
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ray.air.checkpoint import Checkpoint
from ray.util import log_once
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

    Attributes:
        metrics: The final metrics as reported by an Trainable.
        checkpoint: The final checkpoint of the Trainable.
        error: The execution error of the Trainable run, if the trial finishes in error.
        local_path: Path pointing to the experiment directory on the local disk.
        remote_path: Path pointing to the experiment directory on remote storage,
            if configured.
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
    local_path: Optional[str]
    remote_path: Optional[str] = None
    metrics_dataframe: Optional["pd.DataFrame"] = None
    best_checkpoints: Optional[List[Tuple[Checkpoint, Dict[str, Any]]]] = None
    _items_to_repr = ["error", "metrics", "log_dir", "checkpoint"]
    # Deprecate: raise in 2.5, remove in 2.6
    log_dir: Optional[Path] = None

    def __post_init__(self):
        if self.log_dir and log_once("result_log_dir_deprecated"):
            warnings.warn(
                "The `Result.log_dir` property is deprecated. "
                "Use `local_path` instead."
            )
            self.local_path = str(self.log_dir)

        # Duplicate for retrieval
        self.log_dir = Path(self.local_path)
        # Backwards compatibility: Make sure to cast Path to string
        # Deprecate: Remove this line after 2.6
        self.local_path = str(self.local_path)

    @property
    def config(self) -> Optional[Dict[str, Any]]:
        """The config associated with the result."""
        if not self.metrics:
            return None
        return self.metrics.get("config", None)

    @property
    def path(self) -> str:
        """Path pointing to the experiment directory.

        If remote storage is configured, will point to the ``remote_path``.
        Otherwise, will point to the ``local_path``.
        """
        return self.remote_path or self.local_path

    def _repr(self, indent: int = 0) -> str:
        """Construct the representation with specified number of space indent."""
        from ray.tune.result import AUTO_RESULT_KEYS

        shown_attributes = {k: self.__dict__[k] for k in self._items_to_repr}
        if self.error:
            shown_attributes["error"] = type(self.error).__name__
        else:
            shown_attributes.pop("error")

        if self.metrics:
            shown_attributes["metrics"] = {
                k: v for k, v in self.metrics.items() if k not in AUTO_RESULT_KEYS
            }

        cls_indent = " " * indent
        kws_indent = " " * (indent + 2)

        kws = [
            f"{kws_indent}{key}={value!r}" for key, value in shown_attributes.items()
        ]
        kws_repr = ",\n".join(kws)
        return "{0}{1}(\n{2}\n{0})".format(cls_indent, type(self).__name__, kws_repr)

    def __repr__(self) -> str:
        return self._repr(indent=0)
