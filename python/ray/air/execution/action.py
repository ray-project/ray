from dataclasses import dataclass

from typing import List

import ray


@dataclass
class Action:
    pass


class Continue(Action):
    futures: List[ray.ObjectRef]


class Stop(Action):
    pass
