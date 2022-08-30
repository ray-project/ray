from dataclasses import dataclass

from typing import List

import ray


@dataclass
class Action:
    pass


@dataclass
class Continue(Action):
    futures: List[ray.ObjectRef]


@dataclass
class Stop(Action):
    pass
