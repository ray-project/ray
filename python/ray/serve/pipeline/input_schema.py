from pydantic import (
    BaseModel,
    validator,
)
from typing import Any
import numpy as np

class InputSchemaBase(BaseModel):
    def __init__(self, input_type):
        pass

    def validate(self, user_input: Any):
        pass

    def convert(self, user_input: Any):
        return user_input


class PayloadToNumpy(InputSchemaBase):
    def convert(self, payload: Any):
        return np.array(payload.array)