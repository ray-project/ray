from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class RequestContext:
    model_id: str
    request_id: Optional[str] = None
    session_id: Optional[str] = None
    max_tokens: Optional[int] = None
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None
    estimated_input_tokens: Optional[int] = None
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class BlockedResponse:
    decision: str = "BLOCKED"
    rule_triggered: str = ""
    reason: str = ""
    severity: str = "ERROR"
    retry_after: Optional[int] = None

    def __post_init__(self) -> None:
        if self.decision not in ("BLOCKED", "THROTTLED"):
            raise ValueError(f"Invalid decision: {self.decision!r}")
        if self.severity not in ("ERROR", "WARNING"):
            raise ValueError(f"Invalid severity: {self.severity!r}")
        if self.decision == "THROTTLED" and self.retry_after is None:
            raise ValueError("THROTTLED blocks require retry_after")
