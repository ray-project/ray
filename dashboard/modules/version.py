from dataclasses import dataclass

# Version 0 -> 1: Added log streaming and changed behavior of job logs cli.
# Version 1 -> 2: - Renamed job_id to submission_id.
#                 - Changed list_jobs sdk/cli/api to return a list
#                   instead of a dictionary.
CURRENT_VERSION = "2"


@dataclass
class VersionResponse:
    version: str
    ray_version: str
    ray_commit: str
