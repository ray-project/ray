from dataclasses import dataclass
from typing import Optional, Dict


@dataclass
class Result:
    results: Optional[Dict] = None

    status: str = "invalid"
    last_logs: Optional[str] = None

    runtime: Optional[float] = None
    stable: bool = True

    buildkite_url: Optional[str] = None
    wheels_url: Optional[str] = None
    cluster_url: Optional[str] = None
