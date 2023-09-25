import dataclasses
import logging
import os
import re
import traceback
from dataclasses import dataclass
from typing import Iterator, List, Optional, Any, Dict, Tuple, Union

try:
    # package `aiohttp` is not in ray's minimal dependencies
    import aiohttp
    from aiohttp.web import Request, Response
except Exception:
    aiohttp = None
    Request = None
    Response = None

from ray._private import ray_constants
from ray._private.gcs_utils import GcsAioClient
from ray.dashboard.modules.job.common import (
    validate_request_type,
    JobInfoStorageClient,
)

try:
    # package `pydantic` is not in ray's minimal dependencies
    from ray.dashboard.modules.job.pydantic_models import (
        DriverInfo,
        JobDetails,
        JobType,
    )
except Exception:
    DriverInfo = None
    JobDetails = None
    JobType = None

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


async def parse_and_validate_request(
    req: Request, request_type: dataclass
) -> Union[dataclass, Response]:
    """Parse request and cast to request type.

    Remove keys with value None to allow newer client versions with new optional fields
    to work with older servers.

    If parsing failed, return a Response object with status 400 and stacktrace instead.

    Args:
        req: aiohttp request object.
        request_type: dataclass type to cast request to.

    Returns:
        Parsed request object or Response object with status 400 and stacktrace.
    """
    import aiohttp

    json_data = strip_keys_with_value_none(await req.json())
    try:
        return validate_request_type(json_data, request_type)
    except Exception as e:
        logger.info(f"Got invalid request type: {e}")
        return Response(
            text=traceback.format_exc(),
            status=aiohttp.web.HTTPBadRequest.status_code,
        )


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
