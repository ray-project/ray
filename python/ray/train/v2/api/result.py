import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import pyarrow

import ray
from ray.air.result import Result as ResultV1
from ray.train.v2.api.exceptions import TrainingFailedError
from ray.util.annotations import Deprecated, PublicAPI

logger = logging.getLogger(__name__)


@dataclass
class Result(ResultV1):
    checkpoint: Optional["ray.train.Checkpoint"]
    error: Optional[TrainingFailedError]
    best_checkpoints: Optional[
        List[Tuple["ray.train.Checkpoint", Dict[str, Any]]]
    ] = None

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
        raise NotImplementedError("`Result.from_path` is not implemented yet.")

    @property
    @Deprecated
    def config(self) -> Optional[Dict[str, Any]]:
        raise DeprecationWarning(
            "The `config` property for a `ray.train.Result` is deprecated, "
            "since it is only relevant in the context of Ray Tune."
        )
