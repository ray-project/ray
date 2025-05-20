import asyncio
import dataclasses
import logging
import os
import re
import traceback
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Union

from ray._private import ray_constants
from ray.dashboard.modules.job.common import (
    JOB_ID_METADATA_KEY,
    JobInfoStorageClient,
    JobStatus,
    validate_request_type,
)
from ray.dashboard.modules.job.pydantic_models import DriverInfo, JobDetails, JobType
from ray.runtime_env import RuntimeEnv
from ray._raylet import GcsClient

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


def parse_job_resources_json(
    resources: str, cli_logger, cf, command_arg="--job-resources"
) -> Dict[str, float]:
    try:
        import json

        resources = json.loads(resources)
        if not isinstance(resources, dict):
            raise ValueError
    except Exception:
        cli_logger.error("`{}` is not a valid JSON string.", cf.bold(command_arg))
        cli_logger.abort(
            "Valid values look like this: `{}`",
            cf.bold(f'{command_arg}=\'{{"replica": 8, ' '"bundle": {"GPU":4}}}\''),
        )
    return resources


class JobQueueFullException(Exception):
    def __init__(self, message):
        self.message = message


# https://stackoverflow.com/questions/29296064/python-asyncio-how-to-create-and-cancel-tasks-from-another-thread
def execute_in_event_loop(coro, loop):
    def _async_add(func, fut):
        try:
            ret = func()
            fut.set_result(ret)
        except Exception as e:
            fut.set_exception(e)

    f = functools.partial(
        asyncio.ensure_future,
        coro,
        loop=loop,
    )
    fut = Future()
    loop.call_soon_threadsafe(_async_add, f, fut)
    return fut.result()


def generate_job_id() -> str:
    """Returns a job_id of the form 'raysubmit_XYZ'.

    Prefixed with 'raysubmit' to avoid confusion with Ray JobID (driver ID).
    """
    rand = random.SystemRandom()
    possible_characters = list(
        set(string.ascii_letters + string.digits)
        - {"I", "l", "o", "O", "0"}  # No confusing characters
    )
    id_part = "".join(rand.choices(possible_characters, k=16))
    return f"raysubmit_{id_part}"


def encrypt_aes(key, raw, now=None):
    try:
        from Crypto.Cipher import AES
        from Crypto import Random
        from binascii import hexlify
    except Exception:
        return None

    length = 16
    count = len(raw)
    add = length - (count % length)
    raw = raw + ("\0" * add)
    iv = Random.new().read(AES.block_size)
    aes_key = (key[:8] + now) if now else key
    cipher = AES.new(aes_key.encode(), AES.MODE_CBC, iv)
    result = iv + cipher.encrypt(raw.encode())
    return hexlify(result)


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
