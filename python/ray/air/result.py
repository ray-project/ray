import io
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import pyarrow

import ray
from ray.air.constants import (
    EXPR_ERROR_PICKLE_FILE,
    EXPR_PROGRESS_FILE,
    EXPR_RESULT_FILE,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@dataclass
@PublicAPI(stability="stable")
class Result:
    """The final result of a ML training run or a Tune trial.

    This is the output produced by ``Trainer.fit``.
    ``Tuner.fit`` outputs a :class:`~ray.tune.ResultGrid` that is a collection
    of ``Result`` objects.

    This API is the recommended way to access the outputs such as:
    - checkpoints (``Result.checkpoint``)
    - the history of reported metrics (``Result.metrics_dataframe``, ``Result.metrics``)
    - errors encountered during a training run (``Result.error``)

    The constructor is a private API -- use ``Result.from_path`` to create a result
    object from a directory.

    Attributes:
        metrics: The latest set of reported metrics.
        checkpoint: The latest checkpoint.
        error: The execution error of the Trainable run, if the trial finishes in error.
        path: Path pointing to the result directory on persistent storage. This can
            point to a remote storage location (e.g. S3) or to a local location (path
            on the head node). The path is accessible via the result's associated
            `filesystem`. For instance, for a result stored in S3 at
            ``s3://bucket/location``, ``path`` will have the value ``bucket/location``.
        metrics_dataframe: The full result dataframe of the Trainable.
            The dataframe is indexed by iterations and contains reported
            metrics. Note that the dataframe columns are indexed with the
            *flattened* keys of reported metrics, so the format of this dataframe
            may be slightly different than ``Result.metrics``, which is an unflattened
            dict of the latest set of reported metrics.
        best_checkpoints: A list of tuples of the best checkpoints and
            their associated metrics. The number of
            saved checkpoints is determined by :class:`~ray.train.CheckpointConfig`
            (by default, all checkpoints will be saved).
    """

    metrics: Optional[Dict[str, Any]]
    checkpoint: Optional["ray.tune.Checkpoint"]
    error: Optional[Exception]
    path: str
    metrics_dataframe: Optional["pd.DataFrame"] = None
    best_checkpoints: Optional[
        List[Tuple["ray.tune.Checkpoint", Dict[str, Any]]]
    ] = None
    _storage_filesystem: Optional[pyarrow.fs.FileSystem] = None
    _items_to_repr = ["error", "metrics", "path", "filesystem", "checkpoint"]

    @property
    def config(self) -> Optional[Dict[str, Any]]:
        """The config associated with the result."""
        if not self.metrics:
            return None
        return self.metrics.get("config", None)

    @property
    def filesystem(self) -> pyarrow.fs.FileSystem:
        """Return the filesystem that can be used to access the result path.

        Returns:
            pyarrow.fs.FileSystem implementation.
        """
        return self._storage_filesystem or pyarrow.fs.LocalFileSystem()

    def _repr(self, indent: int = 0) -> str:
        """Construct the representation with specified number of space indent."""
        from ray.tune.experimental.output import BLACKLISTED_KEYS
        from ray.tune.result import AUTO_RESULT_KEYS

        shown_attributes = {k: getattr(self, k) for k in self._items_to_repr}
        if self.error:
            shown_attributes["error"] = type(self.error).__name__
        else:
            shown_attributes.pop("error")

        shown_attributes["filesystem"] = shown_attributes["filesystem"].type_name

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
    def _read_file_as_str(
        storage_filesystem: pyarrow.fs.FileSystem,
        storage_path: str,
    ) -> str:
        """Opens a file as an input stream reading all byte content sequentially and
         decoding read bytes as utf-8 string.

        Args:
            storage_filesystem: The filesystem to use.
            storage_path: The source to open for reading.
        """

        with storage_filesystem.open_input_stream(storage_path) as f:
            return f.readall().decode()

    @classmethod
    def from_path(
        cls,
        path: Union[str, os.PathLike],
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> "Result":
        """Restore a Result object from local or remote trial directory.

        Args:
            path: A path of a trial directory on local or remote storage
                (ex: s3://bucket/path or /tmp/ray_results).
            storage_filesystem: A custom filesystem to use. If not provided,
                this will be auto-resolved by pyarrow. If provided, the path
                is assumed to be prefix-stripped already, and must be a valid path
                on the filesystem.

        Returns:
            A :py:class:`Result` object of that trial.
        """
        # TODO(justinvyu): Fix circular dependency.
        from ray.train import Checkpoint
        from ray.train._internal.storage import (
            _exists_at_fs_path,
            _list_at_fs_path,
            get_fs_and_path,
        )
        from ray.train.constants import CHECKPOINT_DIR_NAME

        fs, fs_path = get_fs_and_path(path, storage_filesystem)
        if not _exists_at_fs_path(fs, fs_path):
            raise RuntimeError(f"Trial folder {fs_path} doesn't exist!")

        # Restore metrics from result.json
        result_json_file = Path(fs_path, EXPR_RESULT_FILE).as_posix()
        progress_csv_file = Path(fs_path, EXPR_PROGRESS_FILE).as_posix()
        if _exists_at_fs_path(fs, result_json_file):
            lines = cls._read_file_as_str(fs, result_json_file).split("\n")
            json_list = [json.loads(line) for line in lines if line]
            metrics_df = pd.json_normalize(json_list, sep="/")
            latest_metrics = json_list[-1] if json_list else {}
        # Fallback to restore from progress.csv
        elif _exists_at_fs_path(fs, progress_csv_file):
            metrics_df = pd.read_csv(
                io.StringIO(cls._read_file_as_str(fs, progress_csv_file))
            )
            latest_metrics = (
                metrics_df.iloc[-1].to_dict() if not metrics_df.empty else {}
            )
        else:
            raise RuntimeError(
                f"Failed to restore the Result object: Neither {EXPR_RESULT_FILE}"
                f" nor {EXPR_PROGRESS_FILE} exists in the trial folder!"
            )

        # Restore all checkpoints from the checkpoint folders
        checkpoint_dir_names = sorted(
            _list_at_fs_path(
                fs,
                fs_path,
                file_filter=lambda file_info: file_info.type
                == pyarrow.fs.FileType.Directory
                and file_info.base_name.startswith("checkpoint_"),
            )
        )

        if checkpoint_dir_names:
            checkpoints = [
                Checkpoint(
                    path=Path(fs_path, checkpoint_dir_name).as_posix(), filesystem=fs
                )
                for checkpoint_dir_name in checkpoint_dir_names
            ]

            metrics = []
            for checkpoint_dir_name in checkpoint_dir_names:
                metrics_corresponding_to_checkpoint = metrics_df[
                    metrics_df[CHECKPOINT_DIR_NAME] == checkpoint_dir_name
                ]
                if metrics_corresponding_to_checkpoint.empty:
                    logger.warning(
                        "Could not find metrics corresponding to "
                        f"{checkpoint_dir_name}. These will default to an empty dict."
                    )
                metrics.append(
                    {}
                    if metrics_corresponding_to_checkpoint.empty
                    else metrics_corresponding_to_checkpoint.iloc[-1].to_dict()
                )

            latest_checkpoint = checkpoints[-1]
            # TODO(justinvyu): These are ordered by checkpoint index, since we don't
            # know the metric to order these with.
            best_checkpoints = list(zip(checkpoints, metrics))
        else:
            best_checkpoints = latest_checkpoint = None

        # Restore the trial error if it exists
        error = None
        error_file_path = Path(fs_path, EXPR_ERROR_PICKLE_FILE).as_posix()
        if _exists_at_fs_path(fs, error_file_path):
            with fs.open_input_stream(error_file_path) as f:
                error = ray.cloudpickle.load(f)

        return Result(
            metrics=latest_metrics,
            checkpoint=latest_checkpoint,
            path=fs_path,
            _storage_filesystem=fs,
            metrics_dataframe=metrics_df,
            best_checkpoints=best_checkpoints,
            error=error,
        )

    @PublicAPI(stability="alpha")
    def get_best_checkpoint(
        self, metric: str, mode: str
    ) -> Optional["ray.tune.Checkpoint"]:
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
