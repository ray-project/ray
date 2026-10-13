"""The output of the Decide stage: one instruction for the controller."""
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, ClassVar, List

from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.train.health.probe import OnDemandProbe


@DeveloperAPI
class Action(Enum):
    NOOP = auto()
    DIAGNOSE = auto()
    REATTEMPT = auto()
    EVICT = auto()


SEVERITY = {Action.NOOP: 0, Action.DIAGNOSE: 1, Action.REATTEMPT: 2, Action.EVICT: 3}


@DeveloperAPI
class Cause(Enum):
    UNKNOWN = auto()
    HARDWARE = auto()
    INFRASTRUCTURE = auto()
    APPLICATION = auto()
    NO_PROGRESS = auto()


@DeveloperAPI
@dataclass
class HealthDecision:
    action: ClassVar[Action]
    cause: Cause = Cause.UNKNOWN
    reason: str = ""


@DeveloperAPI
@dataclass
class Noop(HealthDecision):
    action: ClassVar[Action] = Action.NOOP


@DeveloperAPI
@dataclass
class Reattempt(HealthDecision):
    action: ClassVar[Action] = Action.REATTEMPT


@DeveloperAPI
@dataclass
class Evict(HealthDecision):
    action: ClassVar[Action] = Action.EVICT
    target_nodes: List[str] = field(default_factory=list)


@DeveloperAPI
@dataclass
class Diagnose(HealthDecision):
    action: ClassVar[Action] = Action.DIAGNOSE
    on_demand_probes: List["OnDemandProbe"] = field(default_factory=list)
    target_nodes: List[str] = field(default_factory=list)
    target_ranks: List[int] = field(default_factory=list)
