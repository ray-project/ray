from enum import Enum

from ray.serve.pipeline.step import step

class InputFlag(Enum):
    INPUT_FLAG = 1

INPUT = InputFlag.INPUT_FLAG
