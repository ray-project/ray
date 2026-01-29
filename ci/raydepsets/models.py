from dataclasses import dataclass
from typing import List


@dataclass
class Dep:
    """Represents a dependency with its name, version, and dependents."""

    name: str
    version: str
    required_by: List[str]
