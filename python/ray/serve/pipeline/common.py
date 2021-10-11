from enum import Enum

from pydantic import BaseModel, PositiveInt


class ExecutionMode(Enum):
    LOCAL = 1
    TASKS = 2
    ACTORS = 3


class StepConfig(BaseModel):
    execution_mode: ExecutionMode
    num_replicas: PositiveInt
