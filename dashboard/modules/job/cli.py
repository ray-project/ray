import logging
import os
from typing import Optional, Tuple

import click

from ray.dashboard.modules.job.sdk import JobSubmissionClient

logger = logging.getLogger(__name__)


def _get_sdk_client(address: Optional[str]) -> JobSubmissionClient:
    if address is None:
        if "RAY_ADDRESS" not in os.environ:
            raise ValueError(
                "Address must be specified using either the --address flag "
                "or RAY_ADDRESS environment variable.")
        address = os.environ["RAY_ADDRESS"]

    return JobSubmissionClient(address)


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
)
@click.option(
    "--job-id",
    type=str,
    default=None,
    required=False,
)
@click.option(
    "--working-dir",
    type=str,
    default=None,
    required=False,
)
@click.argument("entrypoint", nargs=-1, required=True, type=click.UNPROCESSED)
def job_submit(address: Optional[str], job_id: Optional[str],
               working_dir: Optional[str], entrypoint: Tuple[str]):
    """Submits a job to be run on the cluster.

    Example:
        >>> ray job submit -- python my_script.py --arg=val
    """
    client = _get_sdk_client(address)

    runtime_env = {}
    if working_dir is not None:
        runtime_env["working_dir"] = working_dir

    job_id = client.submit_job(
        entrypoint=" ".join(entrypoint),
        job_id=job_id,
        runtime_env=runtime_env)
    logger.info(f"Job submitted successfully: {job_id}.")
    logger.info(
        f"Query the status of the job using: `ray job status {job_id}`.")


@job_cli_group.command("status", help="Get the status of a running job.")
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
)
@click.argument("job-id", type=str)
def job_status(address: Optional[str], job_id: str):
    """Queries for the current status of a job.

    Example:
        >>> ray job status <my_job_id>
    """
    client = _get_sdk_client(address)
    logger.info(f"Job status for '{job_id}': {client.get_job_status(job_id)}")


@job_cli_group.command("stop", help="Attempt to stop a running job.")
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
)
@click.argument("job-id", type=str)
def job_stop(address: Optional[str], job_id: str):
    """Attempts to stop a job.

    Example:
        >>> ray job stop <my_job_id>
    """
    # TODO(edoakes): should we wait for the job to exit afterwards?
    client = _get_sdk_client(address)
    client.stop_job(job_id)


@job_cli_group.command("logs", help="Get the logs of a running job.")
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
)
@click.argument("job-id", type=str)
def job_logs(address: Optional[str], job_id: str):
    """Gets the logs of a job.

    Example:
        >>> ray job logs <my_job_id>
    """
    client = _get_sdk_client(address)
    print(client.get_job_logs(job_id), end="")
