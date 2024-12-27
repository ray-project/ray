import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import pandas as pd
import pyarrow

from ray.air.result import Result as ResultV1
from ray.train.v2._internal.exceptions import TrainingFailedError

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
        CheckpointManager,
    )

logger = logging.getLogger(__name__)


@dataclass
class Result(ResultV1):
    error: Optional[TrainingFailedError]

    @classmethod
    def from_path(
        cls,
        path: Union[str, os.PathLike],
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> "Result":
        raise NotImplementedError("`Result.from_path` is not implemented yet.")

    @property
    def config(self) -> Optional[Dict[str, Any]]:
        raise DeprecationWarning(
            "The `config` property is deprecated, since it is only "
            "relevant in the context of Ray Tune."
        )


def _build_result(
    checkpoint_manager: "CheckpointManager", error: Optional[TrainingFailedError]
) -> Result:
    storage = checkpoint_manager._storage_context

    latest_checkpoint_result = checkpoint_manager.latest_checkpoint_result
    latest_metrics = (
        latest_checkpoint_result.metrics if latest_checkpoint_result else None
    )
    latest_checkpoint = (
        latest_checkpoint_result.checkpoint if latest_checkpoint_result else None
    )
    best_checkpoints = [
        (r.checkpoint, r.metrics) for r in checkpoint_manager.best_checkpoint_results
    ]

    # Provide the history of metrics attached to checkpoints as a dataframe.
    metrics_dataframe = None
    if best_checkpoints:
        metrics_dataframe = pd.DataFrame([m for _, m in best_checkpoints])

    return Result(
        metrics=latest_metrics,
        checkpoint=latest_checkpoint,
        error=error,
        path=storage.experiment_fs_path,
        best_checkpoints=best_checkpoints,
        metrics_dataframe=metrics_dataframe,
        _storage_filesystem=storage.storage_filesystem,
    )
