from dataclasses import dataclass
from numbers import Number
from typing import Dict, List

STRICT_PACK = "STRICT_PACK"
STRICT_SPREAD = "STRICT_SPREAD"
PACK = "PACK"
SPREAD = "SPREAD"

@dataclass
class PlacementGroupLoad:
    strategy: str
    shapes: List[Dict[str, Number]]
