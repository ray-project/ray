import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

import pandas as pd
import pyarrow

from ray.air.result import Result as ResultV1
from ray.train.v2._internal.exceptions import TrainingFailedError

logger = logging.getLogger(__name__)


@dataclass
class Result(ResultV1):
    error: Optional[TrainingFailedError]
    # The metrics dataframe will not be supported in the new Result class.
    metrics_dataframe: Optional["pd.DataFrame"] = field(init=False, default=None)

    @classmethod
    def from_path(
        cls,
        path: Union[str, os.PathLike],
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> "Result":
        raise NotImplementedError

    @property
    def config(self) -> Optional[Dict[str, Any]]:
        raise NotImplementedError
