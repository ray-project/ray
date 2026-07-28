from dataclasses import dataclass, field
from typing import Dict, Optional


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
    """

    image: str = "python:3.10-slim"
    cpu: float = 1.0
    memory: str = "1Gi"
    env: Dict[str, str] = field(default_factory=dict)
    work_dir: str = "/workspace"
    ttl_seconds: Optional[int] = 3600
    labels: Dict[str, str] = field(default_factory=dict)
    timeout_seconds: float = 30.0
    runsc_path: str = "runsc"
    rootless: bool = True
    network: str = "none"


GVisorSandboxConfig = SandboxConfig
