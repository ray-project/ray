from enum import Enum

from pydantic import BaseModel, PositiveInt


class ExecutionMode(Enum):
    LOCAL = 1
    TASKS = 2
    ACTORS = 3


def str_to_execution_mode(s: str) -> ExecutionMode:
    if s in ["local", "LOCAL"]:
        return ExecutionMode.LOCAL
    elif s in ["TASKS", "tasks"]:
        return ExecutionMode.TASKS
    elif s in ["ACTORS", "actors"]:
        return ExecutionMode.ACTORS
    else:
        valid_strs = [str(mode).split(".")[1] for mode in ExecutionMode]
        raise ValueError(
            f"Unknown ExecutionMode str: '{s}'. " f"Valid options are: {valid_strs}."
        )


class StepConfig(BaseModel):
    execution_mode: ExecutionMode
    num_replicas: PositiveInt
