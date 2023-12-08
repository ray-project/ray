from dataclasses import dataclass
from typing import List, Optional

from pydantic import BaseModel

from ray.cloudpickle import cloudpickle

#
# NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
#
# Payloads in this module are purposefully extracted from benchmark file to force
# Ray's cloudpickle behavior when it does NOT serialize the class definition itself
# along with its payload (instead relying on it being imported)
#


class PayloadPydantic(BaseModel):
    class Error(BaseModel):
        msg: str
        code: int
        type: str

    text: Optional[str] = None
    floats: Optional[List[float]] = None
    ints: Optional[List[int]] = None
    ts: Optional[float] = None
    reason: Optional[str] = None
    error: Optional[Error] = None


@dataclass
class PayloadDataclass:
    @dataclass
    class Error:
        msg: str
        type: str
        code: int

    text: Optional[str] = None
    floats: Optional[List[float]] = None
    ints: Optional[List[int]] = None
    ts: Optional[float] = None
    reason: Optional[str] = None
    error: Optional[Error] = None
