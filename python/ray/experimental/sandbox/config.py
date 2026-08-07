import re
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Union


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
        workdir: Default working directory inside the sandbox. Note that the
            working directory is the only writable path in the sandbox. If not provided,
            the container's WORKDIR is used.
        ttl_seconds: Optional automatic cleanup time-to-live in seconds.
        timeout_seconds: Timeout in seconds for sandbox creation.
        rootless: If True, run gVisor in rootless mode (default: True).
        network: Network mode for runsc ("none", "host", "sandbox") (default: "none").
        readonly: If True, mount container image rootfs in read-only mode (default: True).
    """

    image: str
    cpu: float = 0.0
    memory: Union[str, int, float] = 0
    env: Dict[str, str] = field(default_factory=dict)
    workdir: Optional[str] = None
    ttl_seconds: Optional[int] = 3600
    timeout_seconds: float = 30.0
    rootless: bool = True
    network: str = "none"
    readonly: bool = True
    _oci_spec_transforms: Optional[List[Callable[[Dict], Optional[Dict]]]] = field(
        default=None, repr=False, compare=False
    )

    def __post_init__(self):
        if not self.image or not isinstance(self.image, str) or not self.image.strip():
            raise ValueError("A valid container image name must be specified.")


GVisorSandboxConfig = SandboxConfig
