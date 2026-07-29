from dataclasses import dataclass
from typing import List, Optional

from pydantic import BaseModel

#
# NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
#
# Payloads in this module are purposefully extracted from benchmark file to force
# Ray's cloudpickle behavior when it does NOT serialize the class definition itself
# along with its payload (instead relying on it being imported)
#


class PayloadPydantic(BaseModel):
    text: Optional[str] = None
    floats: Optional[List[float]] = None
    ints: Optional[List[int]] = None
    ts: Optional[float] = None
    reason: Optional[str] = None


@dataclass
class PayloadDataclass:
    text: Optional[str] = None
    floats: Optional[List[float]] = None
    ints: Optional[List[int]] = None
    ts: Optional[float] = None
    reason: Optional[str] = None
