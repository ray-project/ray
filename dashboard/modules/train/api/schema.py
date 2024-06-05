from typing import List

from ray._private.pydantic_compat import BaseModel
from ray.train._internal.state.schema import (
    TrainRunInfo,
)


class TrainRunsResponse(BaseModel):
    train_runs: List[TrainRunInfo]
