import dataclasses
import logging
import os
import re
from typing import Iterator, List, Optional, Any, Dict, Tuple
import ray
import ray._private.services as services
from ray._raylet import GcsClient
from ray._private.utils import split_address
from ray._private import ray_constants
from ray._private.gcs_utils import GcsAioClient
from ray.dashboard.modules.job.common import (
    JobInfoStorageClient,
)
from ray.dashboard.modules.job.pydantic_models import (
    DriverInfo,
    JobDetails,
    JobType,
)
from ray.dashboard.modules.job.common import (
    JobStatus,
    JOB_ID_METADATA_KEY,
)
from ray.runtime_env import RuntimeEnv

logger = logging.getLogger(__name__)

MAX_CHUNK_LINE_LENGTH = 10
MAX_CHUNK_CHAR_LENGTH = 20000


def strip_keys_with_value_none(d: Dict[str, Any]) -> Dict[str, Any]:
    """Strip keys with value None from a dictionary."""
    return {k: v for k, v in d.items() if v is not None}


def redact_url_password(url: str) -> str:
    """Redact any passwords in a URL."""
    secret = re.findall("https?:\/\/.*:(.*)@.*", url)
    if len(secret) > 0:
        url = url.replace(f":{secret[0]}@", ":<redacted>@")

    return url


def file_tail_iterator(path: str) -> Iterator[Optional[List[str]]]:
    """Yield lines from a file as it's written.

    Returns lines in batches of up to 10 lines or 20000 characters,
    whichever comes first. If it's a chunk of 20000 characters, then
    the last line that is yielded could be an incomplete line.
    New line characters are kept in the line string.

    Returns None until the file exists or if no new line has been written.
    """
    if not isinstance(path, str):
        raise TypeError(f"path must be a string, got {type(path)}.")

    while not os.path.exists(path):
        logger.debug(f"Path {path} doesn't exist yet.")
        yield None

    with open(path, "r") as f:
        lines = []
        chunk_char_count = 0
        curr_line = None
        while True:
            if curr_line is None:
                # Only read the next line in the file
                # if there's no remaining "curr_line" to process
                curr_line = f.readline()
            new_chunk_char_count = chunk_char_count + len(curr_line)
            if new_chunk_char_count > MAX_CHUNK_CHAR_LENGTH:
                # Too many characters, return 20000 in this chunk, and then
                # continue loop with remaining characters in curr_line
                truncated_line = curr_line[0 : MAX_CHUNK_CHAR_LENGTH - chunk_char_count]
                lines.append(truncated_line)
                # Set remainder of current line to process next
                curr_line = curr_line[MAX_CHUNK_CHAR_LENGTH - chunk_char_count :]
                yield lines or None
                lines = []
                chunk_char_count = 0
            elif len(lines) >= 9:
                # Too many lines, return 10 lines in this chunk, and then
                # continue reading the file.
                lines.append(curr_line)
                yield lines or None
                lines = []
                chunk_char_count = 0
                curr_line = None
            elif curr_line:
                # Add line to current chunk
                lines.append(curr_line)
                chunk_char_count = new_chunk_char_count
                curr_line = None
            else:
                # readline() returns empty string when there's no new line.
                yield lines or None
                lines = []
                chunk_char_count = 0
                curr_line = None


async def get_driver_jobs(
    gcs_aio_client: GcsAioClient, timeout: Optional[int] = None
) -> Tuple[Dict[str, JobDetails], Dict[str, DriverInfo]]:
    """Returns a tuple of dictionaries related to drivers.

    The first dictionary contains all driver jobs and is keyed by the job's id.
    The second dictionary contains drivers that belong to submission jobs.
    It's keyed by the submission job's submission id.
    Only the last driver of a submission job is returned.
    """
    job_infos = await gcs_aio_client.get_all_job_info(timeout=timeout)

    jobs = {}
    submission_job_drivers = {}
    for job_table_entry in job_infos.values():
        if job_table_entry.config.ray_namespace.startswith(
            ray_constants.RAY_INTERNAL_NAMESPACE_PREFIX
        ):
            # Skip jobs in any _ray_internal_ namespace
            continue
        job_id = job_table_entry.job_id.hex()
        metadata = dict(job_table_entry.config.metadata)
        job_submission_id = metadata.get(JOB_ID_METADATA_KEY)
        if not job_submission_id:
            driver = DriverInfo(
                id=job_id,
                node_ip_address=job_table_entry.driver_address.ip_address,
                pid=str(job_table_entry.driver_pid),
            )
            job = JobDetails(
                job_id=job_id,
                type=JobType.DRIVER,
                status=JobStatus.SUCCEEDED
                if job_table_entry.is_dead
                else JobStatus.RUNNING,
                entrypoint=job_table_entry.entrypoint,
                start_time=job_table_entry.start_time,
                end_time=job_table_entry.end_time,
                metadata=metadata,
                runtime_env=RuntimeEnv.deserialize(
                    job_table_entry.config.runtime_env_info.serialized_runtime_env
                ).to_dict(),
                driver_info=driver,
            )
            jobs[job_id] = job
        else:
            driver = DriverInfo(
                id=job_id,
                node_ip_address=job_table_entry.driver_address.ip_address,
                pid=str(job_table_entry.driver_pid),
            )
            submission_job_drivers[job_submission_id] = driver

    return jobs, submission_job_drivers


async def find_job_by_ids(
    gcs_aio_client: GcsAioClient,
    job_info_client: JobInfoStorageClient,
    job_or_submission_id: str,
) -> Optional[JobDetails]:
    """
    Attempts to find the job with a given submission_id or job id.
    """
    # First try to find by job_id
    driver_jobs, submission_job_drivers = await get_driver_jobs(gcs_aio_client)
    job = driver_jobs.get(job_or_submission_id)
    if job:
        return job
    # Try to find a driver with the given id
    submission_id = next(
        (
            id
            for id, driver in submission_job_drivers.items()
            if driver.id == job_or_submission_id
        ),
        None,
    )

    if not submission_id:
        # If we didn't find a driver with the given id,
        # then lets try to search for a submission with given id
        submission_id = job_or_submission_id

    job_info = await job_info_client.get_info(submission_id)
    if job_info:
        driver = submission_job_drivers.get(submission_id)
        job = JobDetails(
            **dataclasses.asdict(job_info),
            submission_id=submission_id,
            job_id=driver.id if driver else None,
            driver_info=driver,
            type=JobType.SUBMISSION,
        )
        return job

    return None


def ray_address_to_api_server_url(address: Optional[str]) -> str:
    """Parse a Ray cluster address into API server URL.

    When an address is provided, it will be used to query GCS for
    API server address from GCS, so a Ray cluster must be running.

    When an address is not provided, it will first try to auto-detect
    a running Ray instance, or look for local GCS process.

    Args:
        address: Ray cluster bootstrap address or Ray Client address.
            Could also be `auto`.

    Returns:
        API server HTTP URL.
    """

    address = services.canonicalize_bootstrap_address_or_die(address)
    gcs_client = GcsClient(address=address, nums_reconnect_retry=0)

    ray.experimental.internal_kv._initialize_internal_kv(gcs_client)
    api_server_url = ray._private.utils.internal_kv_get_with_retry(
        gcs_client,
        ray_constants.DASHBOARD_ADDRESS,
        namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
        num_retries=20,
    )

    if api_server_url is None:
        raise ValueError(
            (
                "Couldn't obtain the API server address from GCS. It is likely that "
                "the GCS server is down. Check gcs_server.[out | err] to see if it is "
                "still alive."
            )
        )
    api_server_url = f"http://{api_server_url.decode()}"
    return api_server_url


def ray_client_address_to_api_server_url(address: str):
    """Convert a Ray Client address of a running Ray cluster to its API server URL.

    Args:
        address: The Ray Client address, e.g. "ray://my-cluster".

    Returns:
        str: The API server URL of the cluster, e.g. "http://<head-node-ip>:8265".
    """
    with ray.init(address=address) as client_context:
        dashboard_url = client_context.dashboard_url

    return f"http://{dashboard_url}"


def get_address_for_submission_client(address: Optional[str]) -> str:
    """Get Ray API server address from Ray bootstrap or Client address.

    If None, it will try to auto-detect a running Ray instance, or look
    for local GCS process.

    `address` is always overridden by the RAY_ADDRESS environment
    variable, just like the `address` argument in `ray.init()`.

    Args:
        address: Ray cluster bootstrap address or Ray Client address.
            Could also be "auto".

    Returns:
        API server HTTP URL, e.g. "http://<head-node-ip>:8265".
    """
    if os.environ.get("RAY_ADDRESS"):
        logger.debug(f"Using RAY_ADDRESS={os.environ['RAY_ADDRESS']}")
        address = os.environ["RAY_ADDRESS"]

    if address and "://" in address:
        module_string, _ = split_address(address)
        if module_string == "ray":
            logger.debug(
                f"Retrieving API server address from Ray Client address {address}..."
            )
            address = ray_client_address_to_api_server_url(address)
    else:
        # User specified a non-Ray-Client Ray cluster address.
        address = ray_address_to_api_server_url(address)
    logger.debug(f"Using API server address {address}.")
    return address
