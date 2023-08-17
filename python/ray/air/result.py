import os
import json
import pandas as pd
import warnings
from dataclasses import dataclass
from os.path import join
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import ray
from ray.air.checkpoint import Checkpoint
from ray.air.constants import (
    EXPR_RESULT_FILE,
    EXPR_PROGRESS_FILE,
    EXPR_ERROR_PICKLE_FILE,
    CHECKPOINT_TUNE_METADATA_FILE,
)
from ray.util import log_once
from ray.util.annotations import PublicAPI

import logging

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
@dataclass
class Result:
    """The final result of a ML training run or a Tune trial.

    This is the class produced by Trainer.fit().
    It contains a checkpoint, which can be used for resuming training and for
    creating a Predictor object. It also contains a metrics object describing
    training metrics. ``error`` is included so that unsuccessful runs
    and trials can be represented as well.

    The constructor is a private API.

    Attributes:
        metrics: The final metrics as reported by a Trainable.
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
    metrics_dataframe: Optional["pd.DataFrame"] = None
    best_checkpoints: Optional[List[Tuple[Checkpoint, Dict[str, Any]]]] = None
    _local_path: Optional[str] = None
    _remote_path: Optional[str] = None
    _items_to_repr = ["error", "metrics", "path", "checkpoint"]
    # Deprecate: raise in 2.5, remove in 2.6
    log_dir: Optional[Path] = None

    def __post_init__(self):
        if self.log_dir and log_once("result_log_dir_deprecated"):
            warnings.warn(
                "The `Result.log_dir` property is deprecated. "
                "Use `local_path` instead."
            )
            self._local_path = str(self.log_dir)

        # Duplicate for retrieval
        self.log_dir = Path(self._local_path) if self._local_path else None
        # Backwards compatibility: Make sure to cast Path to string
        # Deprecate: Remove this line after 2.6
        self._local_path = str(self._local_path) if self._local_path else None

    @property
    def config(self) -> Optional[Dict[str, Any]]:
        """The config associated with the result."""
        if not self.metrics:
            return None
        return self.metrics.get("config", None)

    @property
    def path(self) -> str:
        """Path pointing to the result directory on persistent storage.

        This can point to a remote storage location (e.g. S3) or to a local
        location (path on the head node).

        For instance, if your remote storage path is ``s3://bucket/location``,
        this will point to ``s3://bucket/location/experiment_name/trial_name``.
        """
        return self._remote_path or self._local_path

    def _repr(self, indent: int = 0) -> str:
        """Construct the representation with specified number of space indent."""
        from ray.tune.result import AUTO_RESULT_KEYS
        from ray.tune.experimental.output import BLACKLISTED_KEYS

        shown_attributes = {k: getattr(self, k) for k in self._items_to_repr}
        if self.error:
            shown_attributes["error"] = type(self.error).__name__
        else:
            shown_attributes.pop("error")

        if self.metrics:
            exclude = set(AUTO_RESULT_KEYS)
            exclude.update(BLACKLISTED_KEYS)
            shown_attributes["metrics"] = {
                k: v for k, v in self.metrics.items() if k not in exclude
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

    @staticmethod
    def _validate_trial_dir(trial_dir: str):
        """Check the validity of the local trial folder."""

        # TODO(yunxuanx): Add more checks for cloud storage restoration
        if not os.path.exists(trial_dir):
            raise RuntimeError(f"Trial folder {trial_dir} doesn't exists!")

    @classmethod
    def from_path(cls, path: str) -> "Result":
        """Restore a Result object from local trial directory.

        Args:
            path: the path to a local trial directory.

        Returns:
            A :py:class:`Result` object of that trial.
        """

        local_path = path
        # TODO(yunxuanx): restoration from cloud storage
        cls._validate_trial_dir(local_path)
        file_list = os.listdir(local_path)

        # Restore metrics from result.json
        if EXPR_RESULT_FILE in file_list:
            with open(Path(local_path) / EXPR_RESULT_FILE, "r") as f:
                json_list = [json.loads(line) for line in f if line]
                metrics_df = pd.json_normalize(json_list, sep="/")
        # Fallback to restore from progress.csv
        elif EXPR_PROGRESS_FILE in file_list:
            metrics_df = pd.read_csv(Path(local_path) / EXPR_PROGRESS_FILE)
        else:
            raise RuntimeError(
                f"Failed to restore the Result object: Neither {EXPR_RESULT_FILE}"
                f" nor {EXPR_PROGRESS_FILE} exists in the trial folder!"
            )

        latest_metrics = metrics_df.iloc[-1].to_dict() if not metrics_df.empty else {}

        # Restore all checkpoints from the checkpoint folders
        ckpt_dirs = [
            join(local_path, entry)
            for entry in file_list
            if entry.startswith("checkpoint_")
        ]

        ckpt_metadata_dicts = [
            ray.cloudpickle.load(
                open(join(ckpt_dir, CHECKPOINT_TUNE_METADATA_FILE), "rb")
            )
            for ckpt_dir in ckpt_dirs
        ]

        if ckpt_dirs:
            checkpoints = [
                Checkpoint.from_directory(ckpt_dir) for ckpt_dir in ckpt_dirs
            ]

            checkpoint_ids = [
                metadata["iteration"] - 1 for metadata in ckpt_metadata_dicts
            ]

            checkpoint_metrics = [
                metadata["last_result"] for metadata in ckpt_metadata_dicts
            ]

            # TODO(air-team): make metrics a property of Checkpoint
            best_checkpoints = list(zip(checkpoints, checkpoint_metrics))
            latest_checkpoint_index = checkpoint_ids.index(max(checkpoint_ids))
            latest_checkpoint = checkpoints[latest_checkpoint_index]
        else:
            best_checkpoints = latest_checkpoint = None

        # Restore the trial error if it exists
        error = None
        error_file_path = Path(local_path) / EXPR_ERROR_PICKLE_FILE
        if error_file_path.exists():
            error = ray.cloudpickle.load(open(error_file_path, "rb"))

        return Result(
            metrics=latest_metrics,
            checkpoint=latest_checkpoint,
            _local_path=local_path,
            _remote_path=None,
            metrics_dataframe=metrics_df,
            best_checkpoints=best_checkpoints,
            error=error,
        )

    @PublicAPI(stability="alpha")
    def get_best_checkpoint(self, metric: str, mode: str) -> Optional[Checkpoint]:
        """Get the best checkpoint from this trial based on a specific metric.

        Any checkpoints without an associated metric value will be filtered out.

        Args:
            metric: The key for checkpoints to order on.
            mode: One of ["min", "max"].

        Returns:
            :class:`Checkpoint <ray.train.Checkpoint>` object, or None if there is
            no valid checkpoint associated with the metric.
        """
        if not self.best_checkpoints:
            raise RuntimeError("No checkpoint exists in the trial directory!")

        if mode not in ["max", "min"]:
            raise ValueError(
                f'Unsupported mode: {mode}. Please choose from ["min", "max"]!'
            )

        op = max if mode == "max" else min
        valid_checkpoints = [
            ckpt_info for ckpt_info in self.best_checkpoints if metric in ckpt_info[1]
        ]

        if not valid_checkpoints:
            raise RuntimeError(
                f"Invalid metric name {metric}! "
                f"You may choose from the following metrics: {self.metrics.keys()}."
            )

        return op(valid_checkpoints, key=lambda x: x[1][metric])[0]
