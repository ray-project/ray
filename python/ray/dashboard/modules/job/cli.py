import json
import os
import pprint
import sys
import time
from subprocess import list2cmdline
from typing import Any, Dict, Optional, Tuple, Union

import click

import ray._private.ray_constants as ray_constants
from ray._private.utils import (
    get_or_create_event_loop,
    load_class,
    parse_metadata_json,
    parse_resources_json,
)
from ray.autoscaler._private.cli_logger import add_click_logging_options, cf, cli_logger
from ray.dashboard.modules.dashboard_sdk import parse_runtime_env_args
from ray.dashboard.modules.job.cli_utils import add_common_job_options
from ray.dashboard.modules.job.utils import redact_url_password
from ray.job_submission import JobStatus, JobSubmissionClient
from ray.util.annotations import PublicAPI


def _get_sdk_client(
    address: Optional[str],
    create_cluster_if_needed: bool = False,
    headers: Optional[str] = None,
    verify: Union[bool, str] = True,
) -> JobSubmissionClient:
    client = JobSubmissionClient(
        address,
        create_cluster_if_needed,
        headers=_handle_headers(headers),
        verify=verify,
    )
    client_address = client.get_address()
    cli_logger.labeled_value(
        "Job submission server address", redact_url_password(client_address)
    )
    return client


def _handle_headers(headers: Optional[str]) -> Optional[Dict[str, Any]]:
    if headers is None and "RAY_JOB_HEADERS" in os.environ:
        headers = os.environ["RAY_JOB_HEADERS"]
    if headers is not None:
        try:
            return json.loads(headers)
        except Exception as exc:
            raise ValueError(
                """Failed to parse headers into JSON.
                Expected format: {{"KEY": "VALUE"}}, got {}, {}""".format(
                    headers, exc
                )
            )
    return None


def _log_big_success_msg(success_msg):
    cli_logger.newline()
    cli_logger.success("-" * len(success_msg))
    cli_logger.success(success_msg)
    cli_logger.success("-" * len(success_msg))
    cli_logger.newline()


def _log_big_error_msg(success_msg):
    cli_logger.newline()
    cli_logger.error("-" * len(success_msg))
    cli_logger.error(success_msg)
    cli_logger.error("-" * len(success_msg))
    cli_logger.newline()


def _log_job_status(client: JobSubmissionClient, job_id: str) -> JobStatus:
    info = client.get_job_info(job_id)
    if info.status == JobStatus.SUCCEEDED:
        _log_big_success_msg(f"Job '{job_id}' succeeded")
    elif info.status == JobStatus.STOPPED:
        cli_logger.warning(f"Job '{job_id}' was stopped")
    elif info.status == JobStatus.FAILED:
        _log_big_error_msg(f"Job '{job_id}' failed")
        if info.message is not None:
            cli_logger.print(f"Status message: {info.message}", no_format=True)
    else:
        # Catch-all.
        cli_logger.print(f"Status for job '{job_id}': {info.status}")
        if info.message is not None:
            cli_logger.print(f"Status message: {info.message}", no_format=True)
    return info.status


async def _tail_logs(client: JobSubmissionClient, job_id: str) -> JobStatus:
    async for lines in client.tail_job_logs(job_id):
        print(lines, end="")

    return _log_job_status(client, job_id)


@click.group("job")
def job_cli_group():
    """Submit, stop, delete, or list Ray jobs."""
    pass


@job_cli_group.command()
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=(
        "Address of the Ray cluster to connect to. Can also be specified "
        "using the RAY_ADDRESS environment variable."
    ),
)
@click.option(
    "--job-id",
    type=str,
    default=None,
    required=False,
    help=("DEPRECATED: Use `--submission-id` instead."),
)
@click.option(
    "--submission-id",
    type=str,
    default=None,
    required=False,
    help=(
        "Submission ID to specify for the job. "
        "If not provided, one will be generated."
    ),
)
@click.option(
    "--runtime-env",
    type=str,
    default=None,
    required=False,
    help="Path to a local YAML file containing a runtime_env definition.",
)
@click.option(
    "--runtime-env-json",
    type=str,
    default=None,
    required=False,
    help="JSON-serialized runtime_env dictionary.",
)
@click.option(
    "--working-dir",
    type=str,
    default=None,
    required=False,
    help=(
        "Directory containing files that your job will run in. Can be a "
        "local directory or a remote URI to a .zip file (S3, GS, HTTP). "
        "If specified, this overrides the option in `--runtime-env`."
    ),
)
@click.option(
    "--metadata-json",
    type=str,
    default=None,
    required=False,
    help="JSON-serialized dictionary of metadata to attach to the job.",
)
@click.option(
    "--entrypoint-num-cpus",
    required=False,
    type=float,
    help="the quantity of CPU cores to reserve for the entrypoint command, "
    "separately from any tasks or actors that are launched by it",
)
@click.option(
    "--entrypoint-num-gpus",
    required=False,
    type=float,
    help="the quantity of GPUs to reserve for the entrypoint command, "
    "separately from any tasks or actors that are launched by it",
)
@click.option(
    "--entrypoint-memory",
    required=False,
    type=int,
    help="the amount of memory to reserve "
    "for the entrypoint command, separately from any tasks or actors that are "
    "launched by it",
)
@click.option(
    "--entrypoint-resources",
    required=False,
    type=str,
    help="a JSON-serialized dictionary mapping resource name to resource quantity "
    "describing resources to reserve for the entrypoint command, "
    "separately from any tasks or actors that are launched by it",
)
@click.option(
    "--no-wait",
    is_flag=True,
    type=bool,
    default=False,
    help="If set, will not stream logs and wait for the job to exit.",
)
@add_common_job_options
@add_click_logging_options
@click.argument("entrypoint", nargs=-1, required=True, type=click.UNPROCESSED)
@PublicAPI
def submit(
    address: Optional[str],
    job_id: Optional[str],
    submission_id: Optional[str],
    runtime_env: Optional[str],
    runtime_env_json: Optional[str],
    metadata_json: Optional[str],
    working_dir: Optional[str],
    entrypoint: Tuple[str],
    entrypoint_num_cpus: Optional[Union[int, float]],
    entrypoint_num_gpus: Optional[Union[int, float]],
    entrypoint_memory: Optional[int],
    entrypoint_resources: Optional[str],
    no_wait: bool,
    verify: Union[bool, str],
    headers: Optional[str],
):
    """Submits a job to be run on the cluster.

    By default (if --no-wait is not set), streams logs to stdout until the job finishes.
    If the job succeeded, exits with 0. If it failed, exits with 1.

    Example:
        `ray job submit -- python my_script.py --arg=val`
    """
    if job_id:
        cli_logger.warning(
            "--job-id option is deprecated. Please use --submission-id instead."
        )
    if entrypoint_resources is not None:
        entrypoint_resources = parse_resources_json(
            entrypoint_resources, cli_logger, cf, command_arg="entrypoint-resources"
        )
    if metadata_json is not None:
        metadata_json = parse_metadata_json(
            metadata_json, cli_logger, cf, command_arg="metadata-json"
        )

    submission_id = submission_id or job_id

    if ray_constants.RAY_JOB_SUBMIT_HOOK in os.environ:
        # Submit all args as **kwargs per the JOB_SUBMIT_HOOK contract.
        load_class(os.environ[ray_constants.RAY_JOB_SUBMIT_HOOK])(
            address=address,
            job_id=submission_id,
            submission_id=submission_id,
            runtime_env=runtime_env,
            runtime_env_json=runtime_env_json,
            metadata_json=metadata_json,
            working_dir=working_dir,
            entrypoint=entrypoint,
            entrypoint_num_cpus=entrypoint_num_cpus,
            entrypoint_num_gpus=entrypoint_num_gpus,
            entrypoint_memory=entrypoint_memory,
            entrypoint_resources=entrypoint_resources,
            no_wait=no_wait,
        )

    client = _get_sdk_client(
        address, create_cluster_if_needed=True, headers=headers, verify=verify
    )

    final_runtime_env = parse_runtime_env_args(
        runtime_env=runtime_env,
        runtime_env_json=runtime_env_json,
        working_dir=working_dir,
    )
    job_id = client.submit_job(
        entrypoint=list2cmdline(entrypoint),
        submission_id=submission_id,
        runtime_env=final_runtime_env,
        metadata=metadata_json,
        entrypoint_num_cpus=entrypoint_num_cpus,
        entrypoint_num_gpus=entrypoint_num_gpus,
        entrypoint_memory=entrypoint_memory,
        entrypoint_resources=entrypoint_resources,
    )

    _log_big_success_msg(f"Job '{job_id}' submitted successfully")

    with cli_logger.group("Next steps"):
        cli_logger.print("Query the logs of the job:")
        with cli_logger.indented():
            cli_logger.print(cf.bold(f"ray job logs {job_id}"))

        cli_logger.print("Query the status of the job:")
        with cli_logger.indented():
            cli_logger.print(cf.bold(f"ray job status {job_id}"))

        cli_logger.print("Request the job to be stopped:")
        with cli_logger.indented():
            cli_logger.print(cf.bold(f"ray job stop {job_id}"))

    cli_logger.newline()
    sdk_version = client.get_version()
    # sdk version 0 does not have log streaming
    if not no_wait:
        if int(sdk_version) > 0:
            cli_logger.print(
                "Tailing logs until the job exits (disable with --no-wait):"
            )
            job_status = get_or_create_event_loop().run_until_complete(
                _tail_logs(client, job_id)
            )
            if job_status == JobStatus.FAILED:
                sys.exit(1)
        else:
            cli_logger.warning(
                "Tailing logs is not enabled for job sdk client version "
                f"{sdk_version}. Please upgrade Ray to the latest version "
                "for this feature."
            )


@job_cli_group.command()
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=(
        "Address of the Ray cluster to connect to. Can also be specified "
        "using the `RAY_ADDRESS` environment variable."
    ),
)
@click.argument("job-id", type=str)
@add_common_job_options
@add_click_logging_options
@PublicAPI(stability="stable")
def status(
    address: Optional[str],
    job_id: str,
    headers: Optional[str],
    verify: Union[bool, str],
):
    """Queries for the current status of a job.

    Example:
        `ray job status <my_job_id>`
    """
    client = _get_sdk_client(address, headers=headers, verify=verify)
    _log_job_status(client, job_id)


@job_cli_group.command()
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=(
        "Address of the Ray cluster to connect to. Can also be specified "
        "using the `RAY_ADDRESS` environment variable."
    ),
)
@click.option(
    "--no-wait",
    is_flag=True,
    type=bool,
    default=False,
    help="If set, will not wait for the job to exit.",
)
@click.argument("job-id", type=str)
@add_common_job_options
@add_click_logging_options
@PublicAPI(stability="stable")
def stop(
    address: Optional[str],
    no_wait: bool,
    job_id: str,
    headers: Optional[str],
    verify: Union[bool, str],
):
    """Attempts to stop a job.

    Example:
        `ray job stop <my_job_id>`
    """
    client = _get_sdk_client(address, headers=headers, verify=verify)
    cli_logger.print(f"Attempting to stop job '{job_id}'")
    client.stop_job(job_id)

    if no_wait:
        return
    else:
        cli_logger.print(
            f"Waiting for job '{job_id}' to exit " f"(disable with --no-wait):"
        )

    while True:
        status = client.get_job_status(job_id)
        if status in {JobStatus.STOPPED, JobStatus.SUCCEEDED, JobStatus.FAILED}:
            _log_job_status(client, job_id)
            break
        else:
            cli_logger.print(f"Job has not exited yet. Status: {status}")
            time.sleep(1)


@job_cli_group.command()
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=(
        "Address of the Ray cluster to connect to. Can also be specified "
        "using the RAY_ADDRESS environment variable."
    ),
)
@click.argument("job-id", type=str)
@add_common_job_options
@add_click_logging_options
@PublicAPI(stability="stable")
def delete(
    address: Optional[str],
    job_id: str,
    headers: Optional[str],
    verify: Union[bool, str],
):
    """Deletes a stopped job and its associated data from memory.

    Only supported for jobs that are already in a terminal state.
    Fails with exit code 1 if the job is not already stopped.
    Does not delete job logs from disk.
    Submitting a job with the same submission ID as a previously
    deleted job is not supported and may lead to unexpected behavior.

    Example:
        ray job delete <my_job_id>
    """
    client = _get_sdk_client(address, headers=headers, verify=verify)
    client.delete_job(job_id)
    cli_logger.print(f"Job '{job_id}' deleted successfully")


@job_cli_group.command()
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=(
        "Address of the Ray cluster to connect to. Can also be specified "
        "using the RAY_ADDRESS environment variable."
    ),
)
@click.argument("job-id", type=str)
@click.option(
    "-f",
    "--follow",
    is_flag=True,
    type=bool,
    default=False,
    help="If set, follow the logs (like `tail -f`).",
)
@add_common_job_options
@add_click_logging_options
@PublicAPI(stability="stable")
def logs(
    address: Optional[str],
    job_id: str,
    follow: bool,
    headers: Optional[str],
    verify: Union[bool, str],
):
    """Gets the logs of a job.

    Example:
        `ray job logs <my_job_id>`
    """
    client = _get_sdk_client(address, headers=headers, verify=verify)
    sdk_version = client.get_version()
    # sdk version 0 did not have log streaming
    if follow:
        if int(sdk_version) > 0:
            get_or_create_event_loop().run_until_complete(_tail_logs(client, job_id))
        else:
            cli_logger.warning(
                "Tailing logs is not enabled for the Jobs SDK client version "
                f"{sdk_version}. Please upgrade Ray to latest version "
                "for this feature."
            )
    else:
        # Set no_format to True because the logs may have unescaped "{" and "}"
        # and the CLILogger calls str.format().
        cli_logger.print(client.get_job_logs(job_id), end="", no_format=True)


@job_cli_group.command()
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=(
        "Address of the Ray cluster to connect to. Can also be specified "
        "using the RAY_ADDRESS environment variable."
    ),
)
@add_common_job_options
@add_click_logging_options
@PublicAPI(stability="stable")
def list(address: Optional[str], headers: Optional[str], verify: Union[bool, str]):
    """Lists all running jobs and their information.

    Example:
        `ray job list`
    """
    client = _get_sdk_client(address, headers=headers, verify=verify)
    # Set no_format to True because the logs may have unescaped "{" and "}"
    # and the CLILogger calls str.format().
    cli_logger.print(pprint.pformat(client.list_jobs()), no_format=True)
