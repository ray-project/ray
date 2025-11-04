import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import pyarrow

import ray
from ray.air.result import Result as ResultV1
from ray.train import Checkpoint, CheckpointConfig
from ray.train.v2._internal.constants import CHECKPOINT_MANAGER_SNAPSHOT_FILENAME
from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
    CheckpointManager,
)
from ray.train.v2._internal.execution.storage import (
    StorageContext,
    _exists_at_fs_path,
    get_fs_and_path,
)
from ray.train.v2.api.exceptions import TrainingFailedError
from ray.util.annotations import Deprecated, PublicAPI

logger = logging.getLogger(__name__)


@dataclass
class Result(ResultV1):
    checkpoint: Optional[Checkpoint]
    error: Optional[TrainingFailedError]
    best_checkpoints: Optional[List[Tuple[Checkpoint, Dict[str, Any]]]] = None

    @PublicAPI(stability="alpha")
    def get_best_checkpoint(
        self, metric: str, mode: str
    ) -> Optional["ray.train.Checkpoint"]:
        return super().get_best_checkpoint(metric, mode)

    @classmethod
    def from_path(
        cls,
        path: Union[str, os.PathLike],
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> "Result":
        """Restore a training result from a previously saved training run path.

        Args:
            path: Path to the run output directory
            storage_filesystem: Optional filesystem to use for accessing the path

        Returns:
            Result object with restored checkpoints and metrics
        """
        fs, fs_path = get_fs_and_path(str(path), storage_filesystem)

        # Validate that the experiment directory exists
        if not _exists_at_fs_path(fs, fs_path):
            raise RuntimeError(f"Experiment folder {fs_path} doesn't exist.")

        # Remove trailing slashes to handle paths correctly
        # os.path.basename() returns empty string for paths with trailing slashes
        fs_path = fs_path.rstrip("/")
        storage_path, experiment_dir_name = os.path.dirname(fs_path), os.path.basename(
            fs_path
        )

        storage_context = StorageContext(
            storage_path=storage_path,
            experiment_dir_name=experiment_dir_name,
            storage_filesystem=fs,
        )

        # Validate that the checkpoint manager snapshot file exists
        if not _exists_at_fs_path(
            storage_context.storage_filesystem,
            storage_context.checkpoint_manager_snapshot_path,
        ):
            raise RuntimeError(
                f"Failed to restore the Result object: "
                f"{CHECKPOINT_MANAGER_SNAPSHOT_FILENAME} doesn't exist in the "
                f"experiment folder. Make sure that this is an output directory created by a Ray Train run."
            )

        checkpoint_manager = CheckpointManager(
            storage_context=storage_context,
            checkpoint_config=CheckpointConfig(),
        )

        # When we build a Result object from checkpoints, the error is not loaded.
        return cls._from_checkpoint_manager(
            checkpoint_manager=checkpoint_manager,
            storage_context=storage_context,
        )

    @classmethod
    def _from_checkpoint_manager(
        cls,
        checkpoint_manager: CheckpointManager,
        storage_context: StorageContext,
        error: Optional[TrainingFailedError] = None,
    ) -> "Result":
        """Create a Result object from a CheckpointManager."""
        latest_checkpoint_result = checkpoint_manager.latest_checkpoint_result
        if latest_checkpoint_result:
            latest_metrics = latest_checkpoint_result.metrics
            latest_checkpoint = latest_checkpoint_result.checkpoint
        else:
            latest_metrics = None
            latest_checkpoint = None
        best_checkpoints = [
            (r.checkpoint, r.metrics)
            for r in checkpoint_manager.best_checkpoint_results
        ]

        # Provide the history of metrics attached to checkpoints as a dataframe.
        metrics_dataframe = None
        if best_checkpoints:
            metrics_dataframe = pd.DataFrame([m for _, m in best_checkpoints])

        return Result(
            metrics=latest_metrics,
            checkpoint=latest_checkpoint,
            error=error,
            path=storage_context.experiment_fs_path,
            best_checkpoints=best_checkpoints,
            metrics_dataframe=metrics_dataframe,
            _storage_filesystem=storage_context.storage_filesystem,
        )

    @property
    @Deprecated
    def config(self) -> Optional[Dict[str, Any]]:
        raise DeprecationWarning(
            "The `config` property for a `ray.train.Result` is deprecated, "
            "since it is only relevant in the context of Ray Tune."
        )
