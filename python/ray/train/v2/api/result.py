import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import pyarrow

from ray.air.result import Result as ResultV1
from ray.train.v2._internal.exceptions import TrainingFailedError
from ray.util.annotations import Deprecated

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

    @Deprecated
    @property
    def config(self) -> Optional[Dict[str, Any]]:
        raise DeprecationWarning(
            "The `config` property for a `ray.train.Result` is deprecated, "
            "since it is only relevant in the context of Ray Tune."
        )
