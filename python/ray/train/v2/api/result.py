import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

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
        storage_path: str,
        experiment_dir_name: str,
    ) -> "Result":
        """Restore a result from a persisted training run directory.

        Args:
            storage_path: Path to the storage directory containing the experiment directory. The storage path can be uri from different filesystems.
            experiment_dir_name: Name of the experiment directory

        Returns:
            Result object with the checkpoints and their attached metrics of a Ray Train run
        """
        # normalize the storage path
        storage_filesystem, storage_path = pyarrow.fs.FileSystem.from_uri(storage_path)

        experiment_path = os.path.join(storage_path, experiment_dir_name)
        if not _exists_at_fs_path(storage_filesystem, experiment_path):
            raise RuntimeError(f"Experiment folder {experiment_path} doesn't exist!")

        storage_context = StorageContext(
            storage_path=storage_path,
            experiment_dir_name=experiment_dir_name,
            storage_filesystem=storage_filesystem,
        )

        # Validate that the checkpoint manager snapshot file exists
        if not _exists_at_fs_path(
            storage_context.storage_filesystem,
            storage_context.checkpoint_manager_snapshot_path,
        ):
            raise RuntimeError(
                "Failed to restore the Result object: "
                f"{CHECKPOINT_MANAGER_SNAPSHOT_FILENAME} doesn't exist in the "
                "experiment folder. Make sure that this is an output directory created "
                "by a Ray Train run."
            )

        checkpoint_manager = CheckpointManager(
            storage_context=storage_context,
            # NOTE: Pass a dummy checkpoint config since the checkpoint
            # manager is only used for loading the checkpoint info, and
            # we will not save further checkpoints.
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
