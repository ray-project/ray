from dataclasses import dataclass, field
from typing import Dict, Any, Callable, Optional, Type, List


@dataclass
class Config:
    type_hooks: Dict[Type, Callable[[Any], Any]] = field(default_factory=dict)
    cast: List[Type] = field(default_factory=list)
    forward_references: Optional[Dict[str, Any]] = None
    check_types: bool = True
    strict: bool = False
    strict_unions_match: bool = False
