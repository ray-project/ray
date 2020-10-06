from dataclasses import dataclass
from numbers import Number
from typing import Dict, List

STRICT_PACK = 1
STRICT_SPREAD = 2
PACK = 3
SPREAD = 4
_strat_enum_to_string = ["PACK", "SPREAD", "STRICT_PACK", "STRICT_SPREAD"]
def strategy_enum_to_string(strat_pb):
    assert strat_pb < len(_strat_enum_to_string)
    return _strat_enum_to_string[strat_pb]


PENDING = 0
CREATED = 1
REMOVED = 2
RESCHEDULING = 3
_state_enum_to_string = ["PENDING", "CREATED", "REMOVED", "RESCHEDULING"]
def state_enum_to_string(state_pb):
    assert state_pb < len(_state_enum_to_string)
    return _state_enum_to_string[state_pb]


@dataclass
class PlacementGroupLoad:
    strategy: int
    state: int
    shapes: List[Dict[str, Number]]
