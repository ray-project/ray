import asyncio
import dataclasses
import logging
import os
import re
import time
import traceback
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from ray._private import ray_constants
from ray._private.gcs_utils import GcsAioClient
from ray.dashboard.modules.job.common import (
    JOB_ID_METADATA_KEY,
    JobInfoStorageClient,
    JobStatus,
    validate_request_type,
)
from ray.dashboard.modules.job.pydantic_models import DriverInfo, JobDetails, JobType
from ray.runtime_env import RuntimeEnv

try:
    # package `aiohttp` is not in ray's minimal dependencies
    import aiohttp
    from aiohttp.web import Request, Response
except Exception:
    aiohttp = None
    Request = None
    Response = None


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

    EOF = ""

    with open(path, "r") as f:
        lines = []

        chunk_char_count = 0
        curr_line = None

        while True:
            # We want to flush current chunk in following cases:
            #   - We accumulated 10 lines
            #   - We accumulated at least MAX_CHUNK_CHAR_LENGTH total chars
            #   - We reached EOF
            if (
                len(lines) >= 10
                or chunk_char_count > MAX_CHUNK_CHAR_LENGTH
                or curr_line == EOF
            ):
                # Too many lines, return 10 lines in this chunk, and then
                # continue reading the file.
                yield lines or None

                lines = []
                chunk_char_count = 0

            # Read next line
            curr_line = f.readline()

            # `readline` will return
            #   - '' for EOF
            #   - '\n' for an empty line in the file
            if curr_line != EOF:
                # Add line to current chunk
                lines.append(curr_line)
                chunk_char_count += len(curr_line)
            else:
                # If EOF is reached sleep for 1s before continuing
                time.sleep(1)


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
    # Sort jobs from GCS to follow convention of returning only last driver
    # of submission job.
    sorted_job_infos = sorted(
        job_infos.values(), key=lambda job_table_entry: job_table_entry.job_id.hex()
    )

    jobs = {}
    submission_job_drivers = {}
    for job_table_entry in sorted_job_infos:
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


async def find_jobs_by_job_ids(
    gcs_aio_client: GcsAioClient,
    job_info_client: JobInfoStorageClient,
    job_ids: List[str],
) -> Dict[str, JobDetails]:
    """
    Returns a dictionary of submission jobs with the given job ids, keyed by the job id.

    This only accepts job ids and not submission ids.
    """
    driver_jobs, submission_job_drivers = await get_driver_jobs(gcs_aio_client)

    # Filter down to the request job_ids
    driver_jobs = {key: job for key, job in driver_jobs.items() if key in job_ids}
    submission_job_drivers = {
        key: job for key, job in submission_job_drivers.items() if job.id in job_ids
    }

    # Fetch job details for each job
    job_submission_ids = submission_job_drivers.keys()
    job_infos = await asyncio.gather(
        *[
            job_info_client.get_info(submission_id)
            for submission_id in job_submission_ids
        ]
    )

    return {
        **driver_jobs,
        **{
            submission_job_drivers.get(submission_id).id: JobDetails(
                **dataclasses.asdict(job_info),
                submission_id=submission_id,
                job_id=submission_job_drivers.get(submission_id).id,
                driver_info=submission_job_drivers.get(submission_id),
                type=JobType.SUBMISSION,
            )
            for job_info, submission_id in zip(job_infos, job_submission_ids)
        },
    }
