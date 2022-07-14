from dataclasses import dataclass

# Version 0 -> 1: Added log streaming and changed behavior of job logs cli.
CURRENT_VERSION = "1"


@dataclass
class VersionResponse:
    version: str
    ray_version: str
    ray_commit: str
