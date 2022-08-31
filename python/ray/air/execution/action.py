from dataclasses import dataclass

from typing import List

from ray.air.execution.future import TypedFuture


@dataclass
class Action:
    pass


@dataclass
class Continue(Action):
    futures: List[TypedFuture]


@dataclass
class Stop(Action):
    pass
