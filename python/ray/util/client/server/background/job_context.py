import json
import os
from typing import Any, Dict, List, Optional

from ray.util.client.server.background.const import ANYSCALE_BACKGROUND_JOB_CONTEXT


class BackgroundJobContext:
    """
    This class represents the runtime context that is passed from the outer job to the inner job for background jobs.
    It is intended to be serialized and passed as an environment variable.
    """

    # Each of these fields represents some metadata that will be attached to the inner job


    # The packaged working directory that this job should use.
    # We use this instead of working directory becuase it's machine agnostic
    runtime_env_uris: List[str]

    # The ray job id of the parent job
    parent_ray_job_id: str

    def __init__(
        self,
        namespace: str,
        runtime_env_uris: List[str],
    ):
        self.namespace = namespace
        self.runtime_env_uris = runtime_env_uris

    def to_json(self) -> str:
        return json.dumps(self.__dict__)

    def to_env_dict(self) -> Dict[str, Any]:
        return {
            ANYSCALE_BACKGROUND_JOB_CONTEXT: self.to_json(),
        }

    @staticmethod
    def from_json(j: str) -> "BackgroundJobContext":
        return BackgroundJobContext(**json.loads(j))

    @staticmethod
    def load_from_env() -> Optional["BackgroundJobContext"]:

        env_var = os.environ.get(ANYSCALE_BACKGROUND_JOB_CONTEXT)
        if not env_var:
            return None
        return BackgroundJobContext.from_json(env_var)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, BackgroundJobContext):
            return self.__dict__ == other.__dict__  # type: ignore
        return False


class BackgroundJob:
    """
    This class is used to manage a backgrounded job.

    Examples:
        >>> bg_job = run(...)
        >>> bg_job.wait() # wait for the job sync
        >>> await bg_job # await the job
        >>> bg_job.kill() # kill the job
    """

    def __init__(self, actor: Any, remote_ref: Any, context: BackgroundJobContext):
        import ray

        self.__ref = remote_ref
        self.__actor = actor
        self.__ray = ray
        self.__context = context

    @property
    def context(self) -> BackgroundJobContext:
        return self.__context

    @property
    def ref(self) -> Any:
        return self.__ref

    def wait(self) -> None:
        self.__ray.get(self.__ref)

    def __await__(self) -> Any:
        return self.__ref.__await__()

    def kill(self) -> None:
        self.__ray.kill(self.__actor)
