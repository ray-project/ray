import json
import logging
import os
from typing import Optional, Tuple
import yaml

import click

from ray.dashboard.modules.job.sdk import JobSubmissionClient

logger = logging.getLogger(__name__)


def _get_sdk_client(address: Optional[str],
                    create_cluster_if_needed: bool = False
                    ) -> JobSubmissionClient:

    if address is None:
        if "RAY_ADDRESS" not in os.environ:
            raise ValueError(
                "Address must be specified using either the --address flag "
                "or RAY_ADDRESS environment variable.")
        address = os.environ["RAY_ADDRESS"]

    logger.info(f"Creating JobSubmissionClient at address: {address}")
    return JobSubmissionClient(address, create_cluster_if_needed)


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
@click.argument("entrypoint", nargs=-1, required=True, type=click.UNPROCESSED)
def job_submit(address: Optional[str], job_id: Optional[str],
               runtime_env: Optional[str], runtime_env_json: Optional[str],
               working_dir: Optional[str], entrypoint: Tuple[str]):
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
            logger.warning(
                "Overriding runtime_env working_dir with --working-dir option."
            )

        final_runtime_env["working_dir"] = working_dir

    job_id = client.submit_job(
        entrypoint=" ".join(entrypoint),
        job_id=job_id,
        runtime_env=final_runtime_env)
    logger.info(f"Job submitted successfully: {job_id}.")
    logger.info(
        f"Query the status of the job using: `ray job status {job_id}`.")


@job_cli_group.command("status", help="Get the status of a running job.")
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=("Address of the Ray cluster to connect to. Can also be specified "
          "using the RAY_ADDRESS environment variable."))
@click.argument("job-id", type=str)
def job_status(address: Optional[str], job_id: str):
    """Queries for the current status of a job.

    Example:
        >>> ray job status <my_job_id>
    """
    client = _get_sdk_client(address)
    status = client.get_job_status(job_id)
    logger.info(f"Job status for '{job_id}': {status.status}.")
    if status.message is not None:
        logger.info(status.message)


@job_cli_group.command("stop", help="Attempt to stop a running job.")
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=("Address of the Ray cluster to connect to. Can also be specified "
          "using the RAY_ADDRESS environment variable."))
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
    help=("Address of the Ray cluster to connect to. Can also be specified "
          "using the RAY_ADDRESS environment variable."))
@click.argument("job-id", type=str)
def job_logs(address: Optional[str], job_id: str):
    """Gets the logs of a job.

    Example:
        >>> ray job logs <my_job_id>
    """
    client = _get_sdk_client(address)
    print(client.get_job_logs(job_id), end="")
