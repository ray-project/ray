import re
from dataclasses import dataclass, field
from typing import Dict, Optional, Union


def parse_memory_bytes(memory: Optional[Union[str, int, float]]) -> Optional[int]:
    """Parse memory specifier string (e.g. '1Gi', '512Mi', '2GB') or number into integer bytes."""
    if memory is None:
        return None
    if isinstance(memory, (int, float)):
        return int(memory)
    if isinstance(memory, str):
        s = memory.strip()
        if not s:
            return None
        if s.isdigit():
            return int(s)

        match = re.match(r"^([0-9.]+)\s*([a-zA-Z]+)?$", s)
        if not match:
            raise ValueError(f"Invalid memory string format: '{memory}'")
        val = float(match.group(1))
        unit = match.group(2)
        if not unit:
            return int(val)
        unit_upper = unit.upper()
        if unit_upper in ("GI", "GIB", "G"):
            multiplier = 1024**3
        elif unit_upper == "GB":
            multiplier = 1000**3
        elif unit_upper in ("MI", "MIB", "M"):
            multiplier = 1024**2
        elif unit_upper == "MB":
            multiplier = 1000**2
        elif unit_upper in ("KI", "KIB", "K"):
            multiplier = 1024
        elif unit_upper == "KB":
            multiplier = 1000
        elif unit_upper == "B":
            multiplier = 1
        else:
            raise ValueError(f"Unknown memory unit in '{memory}'")
        return int(val * multiplier)
    raise TypeError(f"Invalid type for memory: {type(memory)}")


@dataclass
class SandboxConfig:
    """Configuration for a Ray Sandbox instance.

    Attributes:
        image: Container image for the sandbox environment.
        cpu: Number of CPU cores allocated to the sandbox.
        memory: Amount of memory allocated to the sandbox (e.g. "1Gi", "512Mi").
        env: Environment variables to inject into the sandbox.
        work_dir: Default working directory inside the sandbox.
        ttl_seconds: Optional automatic cleanup time-to-live in seconds.
        labels: Optional key-value metadata labels for tracking.
        timeout_seconds: Timeout in seconds for sandbox creation.
        runsc_path: Path to the gVisor `runsc` executable (default: "runsc").
        rootless: If True, run gVisor in rootless mode (default: True).
        network: Network mode for runsc ("none", "host", "sandbox") (default: "none").
        resources: Custom logical resource requirements for the placement actor.
    """

    image: str = "python:3.10-slim"
    cpu: float = 0.0
    memory: Union[str, int, float] = 0
    env: Dict[str, str] = field(default_factory=dict)
    work_dir: str = "/workspace"
    ttl_seconds: Optional[int] = 3600
    labels: Dict[str, str] = field(default_factory=dict)
    timeout_seconds: float = 30.0
    runsc_path: str = "runsc"
    rootless: bool = True
    network: str = "none"
    resources: Dict[str, float] = field(default_factory=dict)


GVisorSandboxConfig = SandboxConfig
