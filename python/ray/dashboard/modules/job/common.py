import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from ray._private import ray_constants
from ray._private.event.export_event_logger import get_export_event_logger
from ray._private.gcs_utils import GcsAioClient
from ray._private.runtime_env.packaging import parse_uri
from ray.core.generated.export_event_pb2 import ExportEvent
from ray.core.generated.export_submission_job_event_pb2 import (
    ExportSubmissionJobEventData,
)
from ray.util.annotations import PublicAPI

# NOTE(edoakes): these constants should be considered a public API because
# they're exposed in the snapshot API.
JOB_ID_METADATA_KEY = "job_submission_id"
JOB_NAME_METADATA_KEY = "job_name"
JOB_ACTOR_NAME_TEMPLATE = (
    f"{ray_constants.RAY_INTERNAL_NAMESPACE_PREFIX}job_actor_" + "{job_id}"
)
# In order to get information about SupervisorActors launched by different jobs,
# they must be set to the same namespace.
SUPERVISOR_ACTOR_RAY_NAMESPACE = "SUPERVISOR_ACTOR_RAY_NAMESPACE"
JOB_LOGS_PATH_TEMPLATE = "job-driver-{submission_id}.log"

logger = logging.getLogger(__name__)


@PublicAPI(stability="stable")
class JobStatus(str, Enum):
    """An enumeration for describing the status of a job."""

    #: The job has not started yet, likely waiting for the runtime_env to be set up.
    PENDING = "PENDING"
    #: The job is currently running.
    RUNNING = "RUNNING"
    #: The job was intentionally stopped by the user.
    STOPPED = "STOPPED"
    #: The job finished successfully.
    SUCCEEDED = "SUCCEEDED"
    #: The job failed.
    FAILED = "FAILED"

    def __str__(self) -> str:
        return f"{self.value}"

    def is_terminal(self) -> bool:
        """Return whether or not this status is terminal.

        A terminal status is one that cannot transition to any other status.
        The terminal statuses are "STOPPED", "SUCCEEDED", and "FAILED".

        Returns:
            True if this status is terminal, otherwise False.
        """
        return self.value in {"STOPPED", "SUCCEEDED", "FAILED"}


# TODO(aguo): Convert to pydantic model
@PublicAPI(stability="stable")
@dataclass
class JobInfo:
    """A class for recording information associated with a job and its execution.

    Please keep this in sync with the JobsAPIInfo proto in src/ray/protobuf/gcs.proto.
    """

    #: The status of the job.
    status: JobStatus
    #: The entrypoint command for this job.
    entrypoint: str
    #: A message describing the status in more detail.
    message: Optional[str] = None
    # TODO(architkulkarni): Populate this field with e.g. Runtime env setup failure,
    #: Internal error, user script error
    error_type: Optional[str] = None
    #: The time when the job was started.  A Unix timestamp in ms.
    start_time: Optional[int] = None
    #: The time when the job moved into a terminal state.  A Unix timestamp in ms.
    end_time: Optional[int] = None
    #: Arbitrary user-provided metadata for the job.
    metadata: Optional[Dict[str, str]] = None
    #: The runtime environment for the job.
    runtime_env: Optional[Dict[str, Any]] = None
    #: The quantity of CPU cores to reserve for the entrypoint command.
    entrypoint_num_cpus: Optional[Union[int, float]] = None
    #: The number of GPUs to reserve for the entrypoint command.
    entrypoint_num_gpus: Optional[Union[int, float]] = None
    #: The amount of memory for workers requesting memory for the entrypoint command.
    entrypoint_memory: Optional[int] = None
    #: The quantity of various custom resources to reserve for the entrypoint command.
    entrypoint_resources: Optional[Dict[str, float]] = None
    #: Driver agent http address
    driver_agent_http_address: Optional[str] = None
    #: The node id that driver running on. It will be None only when the job status
    # is PENDING, and this field will not be deleted or modified even if the driver dies
    driver_node_id: Optional[str] = None
    #: The driver process exit code after the driver executed. Return None if driver
    #: doesn't finish executing
    driver_exit_code: Optional[int] = None

    def __post_init__(self):
        if isinstance(self.status, str):
            self.status = JobStatus(self.status)
        if self.message is None:
            if self.status == JobStatus.PENDING:
                self.message = "Job has not started yet."
                if any(
                    [
                        self.entrypoint_num_cpus is not None
                        and self.entrypoint_num_cpus > 0,
                        self.entrypoint_num_gpus is not None
                        and self.entrypoint_num_gpus > 0,
                        self.entrypoint_memory is not None
                        and self.entrypoint_memory > 0,
                        self.entrypoint_resources not in [None, {}],
                    ]
                ):
                    self.message += (
                        " It may be waiting for resources "
                        "(CPUs, GPUs, memory, custom resources) to become available."
                    )
                if self.runtime_env not in [None, {}]:
                    self.message += (
                        " It may be waiting for the runtime environment to be set up."
                    )
            elif self.status == JobStatus.RUNNING:
                self.message = "Job is currently running."
            elif self.status == JobStatus.STOPPED:
                self.message = "Job was intentionally stopped."
            elif self.status == JobStatus.SUCCEEDED:
                self.message = "Job finished successfully."
            elif self.status == JobStatus.FAILED:
                self.message = "Job failed."

    def to_json(self) -> Dict[str, Any]:
        """Convert this object to a JSON-serializable dictionary.

        Note that the runtime_env field is converted to a JSON-serialized string
        and the field is renamed to runtime_env_json.

        Returns:
            A JSON-serializable dictionary representing the JobInfo object.
        """

        json_dict = asdict(self)

        # Convert enum values to strings.
        json_dict["status"] = str(json_dict["status"])

        # Convert runtime_env to a JSON-serialized string.
        if "runtime_env" in json_dict:
            if json_dict["runtime_env"] is not None:
                json_dict["runtime_env_json"] = json.dumps(json_dict["runtime_env"])
            del json_dict["runtime_env"]

        # Assert that the dictionary is JSON-serializable.
        json.dumps(json_dict)

        return json_dict

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> None:
        """Initialize this object from a JSON dictionary.

        Note that the runtime_env_json field is converted to a dictionary and
        the field is renamed to runtime_env.

        Args:
            json_dict: A JSON dictionary to use to initialize the JobInfo object.
        """
        # Convert enum values to enum objects.
        json_dict["status"] = JobStatus(json_dict["status"])

        # Convert runtime_env from a JSON-serialized string to a dictionary.
        if "runtime_env_json" in json_dict:
            if json_dict["runtime_env_json"] is not None:
                json_dict["runtime_env"] = json.loads(json_dict["runtime_env_json"])
            del json_dict["runtime_env_json"]

        return cls(**json_dict)


class JobInfoStorageClient:
    """
    Interface to put and get job data from the Internal KV store.
    """

    # Please keep this format in sync with JobDataKey()
    # in src/ray/gcs/gcs_server/gcs_job_manager.h.
    JOB_DATA_KEY_PREFIX = f"{ray_constants.RAY_INTERNAL_NAMESPACE_PREFIX}job_info_"
    JOB_DATA_KEY = f"{JOB_DATA_KEY_PREFIX}{{job_id}}"

    def __init__(
        self,
        gcs_aio_client: GcsAioClient,
        export_event_log_dir_root: Optional[str] = None,
    ):
        """
        Initialize the JobInfoStorageClient which manages data in the internal KV store.
        Export Submission Job events are written when the KV store is updated if
        the feature flag is on and a export_event_log_dir_root is passed.
        export_event_log_dir_root doesn't need to be passed if the caller
        is not modifying data in the KV store.
        """
        self._gcs_aio_client = gcs_aio_client
        self._export_submission_job_event_logger: logging.Logger = None
        try:
            if (
                ray_constants.RAY_ENABLE_EXPORT_API_WRITE
                and export_event_log_dir_root is not None
            ):
                self._export_submission_job_event_logger = get_export_event_logger(
                    ExportEvent.SourceType.EXPORT_SUBMISSION_JOB,
                    export_event_log_dir_root,
                )
        except Exception:
            logger.exception(
                "Unable to initialize export event logger so no export "
                "events will be written."
            )

    async def put_info(
        self, job_id: str, job_info: JobInfo, overwrite: bool = True
    ) -> bool:
        """Put job info to the internal kv store.

        Args:
            job_id: The job id.
            job_info: The job info.
            overwrite: Whether to overwrite the existing job info.

        Returns:
            True if a new key is added.
        """
        added_num = await self._gcs_aio_client.internal_kv_put(
            self.JOB_DATA_KEY.format(job_id=job_id).encode(),
            json.dumps(job_info.to_json()).encode(),
            overwrite,
            namespace=ray_constants.KV_NAMESPACE_JOB,
        )
        if added_num == 1 or overwrite:
            # Write export event if data was updated in the KV store
            try:
                self._write_submission_job_export_event(job_id, job_info)
            except Exception:
                logger.exception("Error while writing job submission export event.")
        return added_num == 1

    def _write_submission_job_export_event(
        self, job_id: str, job_info: JobInfo
    ) -> None:
        """
        Write Submission Job export event if _export_submission_job_event_logger
        exists. The logger will exist if the export API feature flag is enabled
        and a log directory was passed to JobInfoStorageClient.
        """
        if not self._export_submission_job_event_logger:
            return

        status_value_descriptor = (
            ExportSubmissionJobEventData.JobStatus.DESCRIPTOR.values_by_name.get(
                job_info.status.name
            )
        )
        if status_value_descriptor is None:
            logger.error(
                f"{job_info.status.name} is not a valid "
                "ExportSubmissionJobEventData.JobStatus enum value. This event "
                "will not be written."
            )
            return
        job_status = status_value_descriptor.number
        submission_event_data = ExportSubmissionJobEventData(
            submission_job_id=job_id,
            status=job_status,
            entrypoint=job_info.entrypoint,
            message=job_info.message,
            metadata=job_info.metadata,
            error_type=job_info.error_type,
            start_time=job_info.start_time,
            end_time=job_info.end_time,
            runtime_env_json=json.dumps(job_info.runtime_env),
            driver_agent_http_address=job_info.driver_agent_http_address,
            driver_node_id=job_info.driver_node_id,
            driver_exit_code=job_info.driver_exit_code,
        )
        self._export_submission_job_event_logger.send_event(submission_event_data)

    async def get_info(self, job_id: str, timeout: int = 30) -> Optional[JobInfo]:
        serialized_info = await self._gcs_aio_client.internal_kv_get(
            self.JOB_DATA_KEY.format(job_id=job_id).encode(),
            namespace=ray_constants.KV_NAMESPACE_JOB,
            timeout=timeout,
        )
        if serialized_info is None:
            return None
        else:
            return JobInfo.from_json(json.loads(serialized_info))

    async def delete_info(self, job_id: str, timeout: int = 30):
        await self._gcs_aio_client.internal_kv_del(
            self.JOB_DATA_KEY.format(job_id=job_id).encode(),
            False,
            namespace=ray_constants.KV_NAMESPACE_JOB,
            timeout=timeout,
        )

    async def put_status(
        self,
        job_id: str,
        status: JobStatus,
        message: Optional[str] = None,
        driver_exit_code: Optional[int] = None,
        jobinfo_replace_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """Puts or updates job status.  Sets end_time if status is terminal."""

        old_info = await self.get_info(job_id)

        if jobinfo_replace_kwargs is None:
            jobinfo_replace_kwargs = dict()
        jobinfo_replace_kwargs.update(
            status=status, message=message, driver_exit_code=driver_exit_code
        )
        if old_info is not None:
            if status != old_info.status and old_info.status.is_terminal():
                assert False, "Attempted to change job status from a terminal state."
            new_info = replace(old_info, **jobinfo_replace_kwargs)
        else:
            new_info = JobInfo(
                entrypoint="Entrypoint not found.", **jobinfo_replace_kwargs
            )

        if status.is_terminal():
            new_info.end_time = int(time.time() * 1000)

        await self.put_info(job_id, new_info)

    async def get_status(self, job_id: str) -> Optional[JobStatus]:
        job_info = await self.get_info(job_id)
        if job_info is None:
            return None
        else:
            return job_info.status

    async def get_all_jobs(self, timeout: int = 30) -> Dict[str, JobInfo]:
        raw_job_ids_with_prefixes = await self._gcs_aio_client.internal_kv_keys(
            self.JOB_DATA_KEY_PREFIX.encode(),
            namespace=ray_constants.KV_NAMESPACE_JOB,
            timeout=timeout,
        )
        job_ids_with_prefixes = [
            job_id.decode() for job_id in raw_job_ids_with_prefixes
        ]
        job_ids = []
        for job_id_with_prefix in job_ids_with_prefixes:
            assert job_id_with_prefix.startswith(
                self.JOB_DATA_KEY_PREFIX
            ), "Unexpected format for internal_kv key for Job submission"
            job_ids.append(job_id_with_prefix[len(self.JOB_DATA_KEY_PREFIX) :])

        async def get_job_info(job_id: str):
            job_info = await self.get_info(job_id, timeout)
            return job_id, job_info

        return {
            job_id: job_info
            for job_id, job_info in await asyncio.gather(
                *[get_job_info(job_id) for job_id in job_ids]
            )
        }


def uri_to_http_components(package_uri: str) -> Tuple[str, str]:
    suffix = Path(package_uri).suffix
    if suffix not in {".zip", ".whl"}:
        raise ValueError(f"package_uri ({package_uri}) does not end in .zip or .whl")
    # We need to strip the <protocol>:// prefix to make it possible to pass
    # the package_uri over HTTP.
    protocol, package_name = parse_uri(package_uri)
    return protocol.value, package_name


def http_uri_components_to_uri(protocol: str, package_name: str) -> str:
    return f"{protocol}://{package_name}"


def validate_request_type(json_data: Dict[str, Any], request_type: dataclass) -> Any:
    return request_type(**json_data)


@dataclass
class JobSubmitRequest:
    # Command to start execution, ex: "python script.py"
    entrypoint: str
    # Optional submission_id to specify for the job. If the submission_id
    # is not specified, one will be generated. If a job with the same
    # submission_id already exists, it will be rejected.
    submission_id: Optional[str] = None
    # DEPRECATED. Use submission_id instead
    job_id: Optional[str] = None
    # Dict to setup execution environment.
    runtime_env: Optional[Dict[str, Any]] = None
    # Metadata to pass in to the JobConfig.
    metadata: Optional[Dict[str, str]] = None
    # The quantity of CPU cores to reserve for the execution
    # of the entrypoint command, separately from any Ray tasks or actors
    # that are created by it.
    entrypoint_num_cpus: Optional[Union[int, float]] = None
    # The quantity of GPUs to reserve for the execution
    # of the entrypoint command, separately from any Ray tasks or actors
    # that are created by it.
    entrypoint_num_gpus: Optional[Union[int, float]] = None
    # The amount of total available memory for workers requesting memory
    # for the execution of the entrypoint command, separately from any Ray
    # tasks or actors that are created by it.
    entrypoint_memory: Optional[int] = None
    # The quantity of various custom resources
    # to reserve for the entrypoint command, separately from any Ray tasks
    # or actors that are created by it.
    entrypoint_resources: Optional[Dict[str, float]] = None
    # Optional virtual cluster ID for job.
    virtual_cluster_id: Optional[str] = None
    # Optional replica sets for job
    replica_sets: Optional[Dict[str, int]] = None

    def __post_init__(self):
        if not isinstance(self.entrypoint, str):
            raise TypeError(f"entrypoint must be a string, got {type(self.entrypoint)}")

        if self.submission_id is not None and not isinstance(self.submission_id, str):
            raise TypeError(
                "submission_id must be a string if provided, "
                f"got {type(self.submission_id)}"
            )

        if self.job_id is not None and not isinstance(self.job_id, str):
            raise TypeError(
                "job_id must be a string if provided, " f"got {type(self.job_id)}"
            )

        if self.runtime_env is not None:
            if not isinstance(self.runtime_env, dict):
                raise TypeError(
                    f"runtime_env must be a dict, got {type(self.runtime_env)}"
                )
            else:
                for k in self.runtime_env.keys():
                    if not isinstance(k, str):
                        raise TypeError(
                            f"runtime_env keys must be strings, got {type(k)}"
                        )

        if self.metadata is not None:
            if not isinstance(self.metadata, dict):
                raise TypeError(f"metadata must be a dict, got {type(self.metadata)}")
            else:
                for k in self.metadata.keys():
                    if not isinstance(k, str):
                        raise TypeError(f"metadata keys must be strings, got {type(k)}")
                for v in self.metadata.values():
                    if not isinstance(v, str):
                        raise TypeError(
                            f"metadata values must be strings, got {type(v)}"
                        )

        if self.entrypoint_num_cpus is not None and not isinstance(
            self.entrypoint_num_cpus, (int, float)
        ):
            raise TypeError(
                "entrypoint_num_cpus must be a number, "
                f"got {type(self.entrypoint_num_cpus)}"
            )

        if self.entrypoint_num_gpus is not None and not isinstance(
            self.entrypoint_num_gpus, (int, float)
        ):
            raise TypeError(
                "entrypoint_num_gpus must be a number, "
                f"got {type(self.entrypoint_num_gpus)}"
            )

        if self.entrypoint_memory is not None and not isinstance(
            self.entrypoint_memory, int
        ):
            raise TypeError(
                "entrypoint_memory must be an integer, "
                f"got {type(self.entrypoint_memory)}"
            )

        if self.entrypoint_resources is not None:
            if not isinstance(self.entrypoint_resources, dict):
                raise TypeError(
                    "entrypoint_resources must be a dict, "
                    f"got {type(self.entrypoint_resources)}"
                )
            else:
                for k in self.entrypoint_resources.keys():
                    if not isinstance(k, str):
                        raise TypeError(
                            "entrypoint_resources keys must be strings, "
                            f"got {type(k)}"
                        )
                for v in self.entrypoint_resources.values():
                    if not isinstance(v, (int, float)):
                        raise TypeError(
                            "entrypoint_resources values must be numbers, "
                            f"got {type(v)}"
                        )

        if self.virtual_cluster_id is not None and not isinstance(
            self.virtual_cluster_id, str
        ):
            raise TypeError(
                "virtual_cluster_id must be a string if provided, "
                f"got {type(self.virtual_cluster_id)}"
            )

        if self.replica_sets is not None:
            if not isinstance(self.replica_sets, dict):
                raise TypeError(
                    "replica_sets must be a dict, " f"got {type(self.replica_sets)}"
                )
            else:
                for k in self.replica_sets.keys():
                    if not isinstance(k, str):
                        raise TypeError(
                            "replica_sets keys must be strings, " f"got {type(k)}"
                        )
                for v in self.replica_sets.values():
                    if not isinstance(v, int):
                        raise TypeError(
                            "replica_sets values must be integers, " f"got {type(v)}"
                        )


@dataclass
class JobSubmitResponse:
    # DEPRECATED: Use submission_id instead.
    job_id: str
    submission_id: str


@dataclass
class JobStopResponse:
    stopped: bool


@dataclass
class JobDeleteResponse:
    deleted: bool


# TODO(jiaodong): Support log streaming #19415
@dataclass
class JobLogsResponse:
    logs: str
