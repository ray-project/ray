#!/usr/bin/env python
import json
import yaml
import os
import sys
import pathlib
import requests
import click
import time

import ray
from ray.serve.api import Deployment, deploy_group, get_deployment_statuses
from ray.serve.config import DeploymentMode
from ray._private.utils import import_attr
from ray import serve
from ray.serve.constants import (
    DEFAULT_CHECKPOINT_PATH,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
)
from ray.dashboard.modules.serve.schema import (
    ServeApplicationSchema,
    schema_to_serve_application,
    serve_application_status_to_schema,
)
from ray.dashboard.modules.dashboard_sdk import SubmissionClient
from ray.autoscaler._private.cli_logger import cli_logger


def log_failed_request(response: requests.models.Response, address: str):
    error_message = (
        f"\nRequest to address {address} failed. Got response status code "
        f"{response.status_code} with the following message:"
        f"\n\n{response.text}"
    )
    cli_logger.newline()
    cli_logger.error(error_message)
    cli_logger.newline()


def configure_working_dir(
    deployment: Deployment, working_dir: str, submission_client: SubmissionClient
) -> None:
    """
    Sets the deployment's working_dir to working_dir. Uses the
    submission_client to upload the working_dir if it's local.
    Mutates the deployment.
    """

    runtime_env = {"working_dir": working_dir}
    submission_client._upload_working_dir_if_needed(runtime_env)
    if deployment.ray_actor_options is None:
        deployment._ray_actor_options = {"runtime_env": runtime_env}
    elif "runtime_env" in deployment.ray_actor_options:
        deployment.ray_actor_options["runtime_env"].update(runtime_env)
    else:
        deployment.ray_actor_options["runtime_env"] = runtime_env


@click.group(help="[EXPERIMENTAL] CLI for managing Serve instances on a Ray cluster.")
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "auto"),
    required=False,
    type=str,
    help="Address of the running Ray cluster to connect to. " 'Defaults to "auto".',
)
@click.option(
    "--namespace",
    "-n",
    default="serve",
    required=False,
    type=str,
    help='Ray namespace to connect to. Defaults to "serve".',
)
@click.option(
    "--runtime-env-json",
    default=r"{}",
    required=False,
    type=str,
    help=("Runtime environment dictionary to pass into ray.init. Defaults to empty."),
)
def cli(address, namespace, runtime_env_json):
    ray.init(
        address=address,
        namespace=namespace,
        runtime_env=json.loads(runtime_env_json),
    )


@cli.command(help="Start a detached Serve instance on the Ray cluster.")
@click.option(
    "--http-host",
    default=DEFAULT_HTTP_HOST,
    required=False,
    type=str,
    help="Host for HTTP servers to listen on. " f"Defaults to {DEFAULT_HTTP_HOST}.",
)
@click.option(
    "--http-port",
    default=DEFAULT_HTTP_PORT,
    required=False,
    type=int,
    help="Port for HTTP servers to listen on. " f"Defaults to {DEFAULT_HTTP_PORT}.",
)
@click.option(
    "--http-location",
    default=DeploymentMode.HeadOnly,
    required=False,
    type=click.Choice(list(DeploymentMode)),
    help="Location of the HTTP servers. Defaults to HeadOnly.",
)
@click.option(
    "--checkpoint-path",
    default=DEFAULT_CHECKPOINT_PATH,
    required=False,
    type=str,
    hidden=True,
)
def start(http_host, http_port, http_location, checkpoint_path):
    serve.start(
        detached=True,
        http_options=dict(
            host=http_host,
            port=http_port,
            location=http_location,
        ),
        _checkpoint_path=checkpoint_path,
    )


@cli.command(help="Shutdown the running Serve instance on the Ray cluster.")
def shutdown():
    serve.api._connect()
    serve.shutdown()


@cli.command(
    help="""
[Experimental]
Create a deployment in running Serve instance. The required argument is the
import path for the deployment: ``my_module.sub_module.file.MyClass``. The
class may or may not be decorated with ``@serve.deployment``.
""",
    hidden=True,
)
@click.argument("deployment")
@click.option(
    "--options-json",
    default=r"{}",
    required=False,
    type=str,
    help="JSON string for the deployments options",
)
def create_deployment(deployment: str, options_json: str):
    deployment_cls = import_attr(deployment)
    if not isinstance(deployment_cls, Deployment):
        deployment_cls = serve.deployment(deployment_cls)
    options = json.loads(options_json)
    deployment_cls.options(**options).deploy()


@cli.command(
    help="""
    [Experimental] Deploy a YAML configuration file via REST API to
    your Serve cluster.
    """,
    hidden=True,
)
@click.argument("config_file_name")
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "http://localhost:8265"),
    required=False,
    type=str,
    help='Address of the Ray dashboard to query. For example, "http://localhost:8265".',
)
def deploy(config_file_name: str, address: str):
    full_address_path = f"{address}/api/serve/deployments/"

    with open(config_file_name, "r") as config_file:
        config = yaml.safe_load(config_file)

    # Generate a schema using the config to ensure its format is valid
    ServeApplicationSchema.parse_obj(config)

    response = requests.put(full_address_path, json=config)

    if response.status_code == 200:
        cli_logger.newline()
        cli_logger.success(
            "\nSent deploy request successfully!\n "
            "* Use `serve status` to check your deployments' statuses.\n "
            "* Use `serve info` to see your running Serve "
            "application's configuration.\n"
        )
        cli_logger.newline()
    else:
        log_failed_request(response, address)


@cli.command(
    help="[Experimental] Run deployments via Serve's Python API.",
    hidden=True,
)
@click.argument("config_or_import_path")
@click.option(
    "--working_dir",
    "-w",
    default=None,
    required=False,
    type=str,
    help="Local path or remote URI of working directory for the deployment(s).",
)
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "http://localhost:8265"),
    required=False,
    type=str,
    help=(
        "Address of the Ray dashboard to query. Only necessary if a "
        "working_dir is specified."
    ),
)
def run(
    config_or_import_path: str,
    working_dir: str,
    address: str,
):
    """
    Deploys deployment(s) from CONFIG_OR_IMPORT_PATH, which must be either a
    Serve YAML configuration file path or an import path to
    a class or function to deploy. Import paths must be of the form
    "module.submodule_1...submodule_n.MyClassOrFunction".
    """

    try:
        # Check if path provided is for config or import
        is_config = pathlib.Path(config_or_import_path).is_file()

        if is_config:

            cli_logger.print(
                "Deploying application in config file at " f"{config_or_import_path}."
            )
            with open(config_or_import_path, "r") as config_file:
                config = yaml.safe_load(config_file)

            schematized_config = ServeApplicationSchema.parse_obj(config)
            deployments = schema_to_serve_application(schematized_config)

            serve.start()
            if working_dir is not None:
                submission_client = SubmissionClient(address)
                for deployment in deployments:
                    configure_working_dir(deployment, working_dir, submission_client)

            deploy_group(deployments)

            cli_logger.newline()
            cli_logger.success(
                f'\nDeployments from config file at "{config_or_import_path}" '
                "deployed successfully!\n"
            )
            cli_logger.newline()

        if not is_config:
            cli_logger.print(
                "Deploying function or class imported from " f"{config_or_import_path}."
            )

            deployment = serve.deployment(name="run")(config_or_import_path)

            serve.start()
            if working_dir is not None:
                submission_client = SubmissionClient(address)
                configure_working_dir(deployment, working_dir, submission_client)

            deployment.deploy()

            cli_logger.newline()
            cli_logger.print(
                f"\nDeployed import at {config_or_import_path} successfully!\n"
            )
            cli_logger.newline()

        while True:
            statuses = serve_application_status_to_schema(
                get_deployment_statuses()
            ).json(indent=4)
            cli_logger.newline()
            cli_logger.print(f"\n{statuses}", no_format=True)
            cli_logger.newline()
            time.sleep(10)

    except KeyboardInterrupt:
        cli_logger.print("Got SIGINT (KeyboardInterrupt). Shutting down Serve.")
        sys.exit()


@cli.command(
    help="[Experimental] Get info about your Serve application's config.",
    hidden=True,
)
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "http://localhost:8265"),
    required=False,
    type=str,
    help='Address of the Ray dashboard to query. For example, "http://localhost:8265".',
)
def info(address: str):
    full_address_path = f"{address}/api/serve/deployments/"
    response = requests.get(full_address_path)
    if response.status_code == 200:
        print(json.dumps(response.json(), indent=4))
    else:
        log_failed_request(response, address)


@cli.command(
    help="[Experimental] Get your Serve application's status.",
    hidden=True,
)
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "http://localhost:8265"),
    required=False,
    type=str,
    help='Address of the Ray dashboard to query. For example, "http://localhost:8265".',
)
def status(address: str):
    full_address_path = f"{address}/api/serve/deployments/status"
    response = requests.get(full_address_path)
    if response.status_code == 200:
        print(json.dumps(response.json(), indent=4))
    else:
        log_failed_request(response, address)


@cli.command(
    help="[Experimental] Get info about your Serve application's config.",
    hidden=True,
)
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "http://localhost:8265"),
    required=False,
    type=str,
    help='Address of the Ray dashboard to query. For example, "http://localhost:8265".',
)
@click.option("--yes", "-y", is_flag=True, help="Bypass confirmation prompt.")
def delete(address: str, yes: bool):
    if not yes:
        click.confirm(
            f"\nThis will shutdown the Serve application at address "
            f'"{address}" and delete all deployments there. Do you '
            "want to continue?",
            abort=True,
        )

    full_address_path = f"{address}/api/serve/deployments/"
    response = requests.delete(full_address_path)
    if response.status_code == 200:
        cli_logger.newline()
        cli_logger.success("\nSent delete request successfully!\n")
        cli_logger.newline()
    else:
        log_failed_request(response, address)
