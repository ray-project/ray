import asyncio
import dataclasses
import logging
import os
import re
import traceback
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Union

from ray._private import ray_constants
from ray._raylet import RAY_INTERNAL_NAMESPACE_PREFIX, GcsClient
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


async def get_head_node_id(gcs_client: GcsClient) -> Optional[str]:
    """Fetches Head node id persisted in GCS"""
    head_node_id_hex_bytes = await gcs_client.async_internal_kv_get(
        ray_constants.KV_HEAD_NODE_ID_KEY,
        namespace=ray_constants.KV_NAMESPACE_JOB,
        timeout=30,
    )
    if head_node_id_hex_bytes is None:
        return None
    return head_node_id_hex_bytes.decode()


def strip_keys_with_value_none(d: Dict[str, Any]) -> Dict[str, Any]:
    """Strip keys with value None from a dictionary."""
    return {k: v for k, v in d.items() if v is not None}


def redact_url_password(url: str) -> str:
    """Redact any passwords in a URL."""
    secret = re.findall(r"https?:\/\/.*:(.*)@.*", url)
    if len(secret) > 0:
        url = url.replace(f":{secret[0]}@", ":<redacted>@")

    return url


async def file_tail_iterator(path: str) -> AsyncIterator[Optional[List[str]]]:
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
                await asyncio.sleep(1)


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
    gcs_client: GcsClient,
    job_or_submission_id: Optional[str] = None,
    timeout: Optional[int] = None,
) -> Tuple[Dict[str, JobDetails], Dict[str, DriverInfo]]:
    """Returns a tuple of dictionaries related to drivers.

    The first dictionary contains all driver jobs and is keyed by the job's id.
    The second dictionary contains drivers that belong to submission jobs.
    It's keyed by the submission job's submission id.
    Only the last driver of a submission job is returned.

    An optional job_or_submission_id filter can be provided to only return
    jobs with the job id or submission id.
    """
    job_infos = await gcs_client.async_get_all_job_info(
        job_or_submission_id=job_or_submission_id,
        skip_submission_job_info_field=True,
        skip_is_running_tasks_field=True,
        timeout=timeout,
    )
    # Sort jobs from GCS to follow convention of returning only last driver
    # of submission job.
    sorted_job_infos = sorted(
        job_infos.values(), key=lambda job_table_entry: job_table_entry.job_id.hex()
    )

    jobs = {}
    submission_job_drivers = {}
    for job_table_entry in sorted_job_infos:
        if job_table_entry.config.ray_namespace.startswith(
            RAY_INTERNAL_NAMESPACE_PREFIX
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
    gcs_client: GcsClient,
    job_info_client: JobInfoStorageClient,
    job_or_submission_id: str,
) -> Optional[JobDetails]:
    """
    Attempts to find the job with a given submission_id or job id.
    """
    # First try to find by job_id
    driver_jobs, submission_job_drivers = await get_driver_jobs(
        gcs_client, job_or_submission_id=job_or_submission_id
    )
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
    gcs_client: GcsClient,
    job_info_client: JobInfoStorageClient,
    job_ids: List[str],
) -> Dict[str, JobDetails]:
    """
    Returns a dictionary of submission jobs with the given job ids, keyed by the job id.

    This only accepts job ids and not submission ids.
    """
    driver_jobs, submission_job_drivers = await get_driver_jobs(gcs_client)

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


def fast_tail_last_n_lines(
    path: str,
    num_lines: int,
    max_chars: int,
    block_size: int = 8192,
) -> str:
    """Return the last ``num_lines`` lines from a large log file efficiently.

    This function avoids scanning the entire file. It seeks to the end of
    the file and reads backwards in fixed-size blocks until enough lines are
    collected. This is much faster for large files compared to using
    ``file_tail_iterator()``, which performs a full sequential scan.

    Args:
        path: The file path to read.
        num_lines: Number of lines to return.
        max_chars: Maximum number of characters in the returned string.
        block_size: Read size for each backward block.

    Returns:
        A string containing at most ``num_lines`` of the last lines in the file,
        truncated to ``max_chars`` characters.
    """
    if num_lines < 0:
        raise ValueError(f"num_lines must be non-negative, got {num_lines}")
    if num_lines == 0:
        return ""
    if max_chars < 0:
        raise ValueError(f"max_chars must be non-negative, got {max_chars}")
    if max_chars == 0:
        return ""
    if block_size <= 0:
        raise ValueError(f"block_size must be positive, got {block_size}")

    logger.debug(
        f"Start reading log file {path} with num_lines={num_lines} max_chars={max_chars} block_size={block_size}"
    )
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        file_size = f.tell()
        if file_size == 0:
            return ""

        chunks = []
        position = file_size
        newlines_found = 0

        # We read backwards in chunks until we have enough newlines for num_lines.
        # We may need one more newline to capture the content before the first newline.
        while position > 0 and newlines_found < num_lines + 1:
            read_size = min(block_size, position)
            position -= read_size
            f.seek(position)

            chunk = f.read(read_size)
            newlines_found += chunk.count(b"\n")
            chunks.insert(0, chunk)

    buffer = b"".join(chunks)
    lines = buffer.decode("utf-8", errors="replace").splitlines(keepends=True)

    if len(lines) <= num_lines:
        result = "".join(lines)
    else:
        result = "".join(lines[-num_lines:])

    return result[-max_chars:]
