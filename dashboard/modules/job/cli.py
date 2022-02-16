import asyncio
import json
import os
from subprocess import list2cmdline
import time
from typing import Optional, Tuple
import yaml

import click

from ray.autoscaler._private.cli_logger import (add_click_logging_options,
                                                cli_logger, cf)
from ray.dashboard.modules.job.common import JobStatus
from ray.dashboard.modules.job.sdk import JobSubmissionClient


def _get_sdk_client(address: Optional[str],
                    create_cluster_if_needed: bool = False
                    ) -> JobSubmissionClient:

    if address is None:
        if "RAY_ADDRESS" not in os.environ:
            raise ValueError(
                "Address must be specified using either the --address flag "
                "or RAY_ADDRESS environment variable.")
        address = os.environ["RAY_ADDRESS"]

    cli_logger.labeled_value("Job submission server address", address)
    return JobSubmissionClient(address, create_cluster_if_needed)


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


def _log_job_status(client: JobSubmissionClient, job_id: str):
    status = client.get_job_status(job_id)
    if status.status == JobStatus.SUCCEEDED:
        _log_big_success_msg(f"Job '{job_id}' succeeded")
    elif status.status == JobStatus.STOPPED:
        cli_logger.warning(f"Job '{job_id}' was stopped")
    elif status.status == JobStatus.FAILED:
        _log_big_error_msg(f"Job '{job_id}' failed")
        if status.message is not None:
            cli_logger.print(f"Status message: {status.message}")
    else:
        # Catch-all.
        cli_logger.print(f"Status for job '{job_id}': {status.status}")
        if status.message is not None:
            cli_logger.print(f"Status message: {status.message}")


async def _tail_logs(client: JobSubmissionClient, job_id: str):
    async for lines in client.tail_job_logs(job_id):
        print(lines, end="")

    _log_job_status(client, job_id)


@click.group("job")
def job_cli_group():
    pass


@job_cli_group.command(
    "submit", help="Submit a job to be executed on the cluster.")
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=("Address of the Ray cluster to connect to. Can also be specified "
          "using the RAY_ADDRESS environment variable."))
@click.option(
    "--job-id",
    type=str,
    default=None,
    required=False,
    help=("Job ID to specify for the job. "
          "If not provided, one will be generated."))
@click.option(
    "--runtime-env",
    type=str,
    default=None,
    required=False,
    help="Path to a local YAML file containing a runtime_env definition.")
@click.option(
    "--runtime-env-json",
    type=str,
    default=None,
    required=False,
    help="JSON-serialized runtime_env dictionary.")
@click.option(
    "--working-dir",
    type=str,
    default=None,
    required=False,
    help=("Directory containing files that your job will run in. Can be a "
          "local directory or a remote URI to a .zip file (S3, GS, HTTP). "
          "If specified, this overrides the option in --runtime-env."),
)
@click.option(
    "--no-wait",
    is_flag=True,
    type=bool,
    default=False,
    help="If set, will not stream logs and wait for the job to exit.")
@add_click_logging_options
@click.argument("entrypoint", nargs=-1, required=True, type=click.UNPROCESSED)
def job_submit(address: Optional[str], job_id: Optional[str],
               runtime_env: Optional[str], runtime_env_json: Optional[str],
               working_dir: Optional[str], entrypoint: Tuple[str],
               no_wait: bool):
    """Submits a job to be run on the cluster.

    Example:
        >>> ray job submit -- python my_script.py --arg=val
    """
    client = _get_sdk_client(address, create_cluster_if_needed=True)

    final_runtime_env = {}
    if runtime_env is not None:
        if runtime_env_json is not None:
            raise ValueError("Only one of --runtime_env and "
                             "--runtime-env-json can be provided.")
        with open(runtime_env, "r") as f:
            final_runtime_env = yaml.safe_load(f)

    elif runtime_env_json is not None:
        final_runtime_env = json.loads(runtime_env_json)

    if working_dir is not None:
        if "working_dir" in final_runtime_env:
            cli_logger.warning(
                "Overriding runtime_env working_dir with --working-dir option")

        final_runtime_env["working_dir"] = working_dir

    job_id = client.submit_job(
        entrypoint=list2cmdline(entrypoint),
        job_id=job_id,
        runtime_env=final_runtime_env,
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
            cli_logger.print("Tailing logs until the job exits "
                             "(disable with --no-wait):")
            asyncio.get_event_loop().run_until_complete(
                _tail_logs(client, job_id))
        else:
            cli_logger.warning(
                "Tailing logs is not enabled for job sdk client version "
                f"{sdk_version}. Please upgrade your ray to latest version "
                "for this feature.")


@job_cli_group.command("status", help="Get the status of a running job.")
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=("Address of the Ray cluster to connect to. Can also be specified "
          "using the RAY_ADDRESS environment variable."))
@click.argument("job-id", type=str)
@add_click_logging_options
def job_status(address: Optional[str], job_id: str):
    """Queries for the current status of a job.

    Example:
        >>> ray job status <my_job_id>
    """
    client = _get_sdk_client(address)
    _log_job_status(client, job_id)


@job_cli_group.command("stop", help="Attempt to stop a running job.")
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=("Address of the Ray cluster to connect to. Can also be specified "
          "using the RAY_ADDRESS environment variable."))
@click.option(
    "--no-wait",
    is_flag=True,
    type=bool,
    default=False,
    help="If set, will not wait for the job to exit.")
@click.argument("job-id", type=str)
@add_click_logging_options
def job_stop(address: Optional[str], no_wait: bool, job_id: str):
    """Attempts to stop a job.

    Example:
        >>> ray job stop <my_job_id>
    """
    client = _get_sdk_client(address)
    cli_logger.print(f"Attempting to stop job {job_id}")
    client.stop_job(job_id)

    if no_wait:
        return
    else:
        cli_logger.print(f"Waiting for job '{job_id}' to exit "
                         f"(disable with --no-wait):")

    while True:
        status = client.get_job_status(job_id)
        if status.status in {
                JobStatus.STOPPED, JobStatus.SUCCEEDED, JobStatus.FAILED
        }:
            _log_job_status(client, job_id)
            break
        else:
            cli_logger.print(f"Job has not exited yet. Status: {status}")
            time.sleep(1)


@job_cli_group.command("logs", help="Get the logs of a running job.")
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=("Address of the Ray cluster to connect to. Can also be specified "
          "using the RAY_ADDRESS environment variable."))
@click.argument("job-id", type=str)
@click.option(
    "-f",
    "--follow",
    is_flag=True,
    type=bool,
    default=False,
    help="If set, follow the logs (like `tail -f`).")
@add_click_logging_options
def job_logs(address: Optional[str], job_id: str, follow: bool):
    """Gets the logs of a job.

    Example:
        >>> ray job logs <my_job_id>
    """
    client = _get_sdk_client(address)
    sdk_version = client.get_version()
    # sdk version 0 did not have log streaming
    if follow:
        if int(sdk_version) > 0:
            asyncio.get_event_loop().run_until_complete(
                _tail_logs(client, job_id))
        else:
            cli_logger.warning(
                "Tailing logs is not enabled for job sdk client version "
                f"{sdk_version}. Please upgrade your ray to latest version "
                "for this feature.")
    else:
        print(client.get_job_logs(job_id), end="")
