from pydantic import BaseModel
from dataclasses import dataclass
from typing import Optional

@dataclass
class JobSpec:
  # Dict to setup execution environment, better to have schema for this
  runtime_env: dict
  # Command to start execution, ex: "python script.py"
  entrypoint: str
  # Metadata to pass in to configure job behavior or use as tags
  # Required by Anyscale product and already supported in Ray drivers
  metadata: dict
  # Likely there will be more fields needed later on for different apps
  # but we should keep it minimal and delegate policies to job manager


class JobSubmitRequest(BaseModel):
   job_spec: JobSpec
   # Globally unique job id. It’s recommended to generate this id from
   # external job manager first, then pass into this API.
   # If job server never had a job running with given id:
   #   - Start new job execution
   # Else if job server has a running job with given id:
   #   - Fail, deployment update and reconfigure should happen in job manager
   job_id: Optional[str] = None


class JobSubmitResponse(BaseModel):
   job_id: str