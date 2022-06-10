import logging

from dataclasses import dataclass
from typing import Optional, List

default_logger = logging.getLogger(__name__)


@dataclass
class Pip():
    packages: List[str]
    pip_check: Optional[bool]

