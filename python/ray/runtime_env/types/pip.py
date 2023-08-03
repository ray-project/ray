from dataclasses import dataclass
from typing import List


@dataclass
class Pip:
    packages: List[str]
    pip_check: bool = False
