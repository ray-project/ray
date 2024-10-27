from dataclasses import dataclass

# Version 0 -> 1: Added log streaming and changed behavior of job logs cli.
# Version 1 -> 2: - Renamed job_id to submission_id.
#                 - Changed list_jobs sdk/cli/api to return a list
#                   instead of a dictionary.
# Version 2 -> 3: - Added optional fields entrypoint_num_cpus, entrypoint_num_gpus
#                   and entrypoint_resources to submit_job sdk/cli/api.
# Version 3 -> 4: - Added DELETE endpoint for deleting jobs.
CURRENT_VERSION = "4"


@dataclass
class VersionResponse:
    version: str
    ray_version: str
    ray_commit: str
    session_name: str
