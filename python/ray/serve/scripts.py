#!/usr/bin/env python
import os
import pathlib
import sys
import time
from typing import Optional, Union

import click
import yaml

import ray
from ray import serve
from ray._private.utils import import_attr
from ray.autoscaler._private.cli_logger import cli_logger
from ray.dashboard.modules.dashboard_sdk import parse_runtime_env_args
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.serve.api import build as build_app
from ray.serve.config import DeploymentMode
from ray.serve.constants import (
    DEFAULT_CHECKPOINT_PATH,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    SERVE_NAMESPACE,
)
from ray.serve.deployment import deployment_to_schema
from ray.serve.deployment_graph import ClassNode, FunctionNode
from ray.serve.schema import ServeApplicationSchema

APP_DIR_HELP_STR = (
    "Local directory to look for the IMPORT_PATH (will be inserted into "
    "PYTHONPATH). Defaults to '.', meaning that an object in ./main.py "
    "can be imported as 'main.object'. Not relevant if you're importing "
    "from an installed module."
)
RAY_INIT_ADDRESS_HELP_STR = (
    "Address to use for ray.init(). Can also be specified "
    "using the RAY_ADDRESS environment variable."
)
RAY_DASHBOARD_ADDRESS_HELP_STR = (
    "Address to use to query the Ray dashboard (defaults to "
    "http://localhost:8265). Can also be specified using the "
    "RAY_ADDRESS environment variable."
)


@click.group(help="CLI for managing Serve instances on a Ray cluster.")
def cli():
    pass


@cli.command(help="Start a detached Serve instance on the Ray cluster.")
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "auto"),
    required=False,
    type=str,
    help=RAY_INIT_ADDRESS_HELP_STR,
)
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
def start(
    address,
    http_host,
    http_port,
    http_location,
    checkpoint_path,
):
    ray.init(
        address=address,
        namespace=SERVE_NAMESPACE,
    )
    serve.start(
        detached=True,
        http_options=dict(
            host=http_host,
            port=http_port,
            location=http_location,
        ),
        _checkpoint_path=checkpoint_path,
    )


@cli.command(
    short_help="Deploy a Serve app from a YAML config file.",
    help=(
        "Deploys deployment(s) from a YAML config file.\n\n"
        "This call is async; a successful response only indicates that the "
        "request was sent to the Ray cluster successfully. It does not mean "
        "the the deployments have been deployed/updated.\n\n"
        "Use `serve config` to fetch the current config and `serve status` to "
        "check the status of the deployments after deploying."
    ),
)
@click.argument("config_file_name")
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "http://localhost:8265"),
    required=False,
    type=str,
    help=RAY_DASHBOARD_ADDRESS_HELP_STR,
)
def deploy(config_file_name: str, address: str):
    with open(config_file_name, "r") as config_file:
        config = yaml.safe_load(config_file)

    # Schematize config to validate format.
    ServeApplicationSchema.parse_obj(config)
    ServeSubmissionClient(address).deploy_application(config)

    cli_logger.newline()
    cli_logger.success(
        "\nSent deploy request successfully!\n "
        "* Use `serve status` to check deployments' statuses.\n "
        "* Use `serve config` to see the running app's config.\n"
    )
    cli_logger.newline()


@cli.command(
    short_help="Run a Serve app.",
    help=(
        "Runs the Serve app from the specified import path or YAML config.\n"
        "Any import path must lead to a FunctionNode or ClassNode object. "
        "By default, this will block and periodically log status. If you "
        "Ctrl-C the command, it will tear down the app."
    ),
)
@click.argument("config_or_import_path")
@click.option(
    "--runtime-env",
    type=str,
    default=None,
    required=False,
    help="Path to a local YAML file containing a runtime_env definition. "
    "This will be passed to ray.init() as the default for deployments.",
)
@click.option(
    "--runtime-env-json",
    type=str,
    default=None,
    required=False,
    help="JSON-serialized runtime_env dictionary. This will be passed to "
    "ray.init() as the default for deployments.",
)
@click.option(
    "--working-dir",
    type=str,
    default=None,
    required=False,
    help=(
        "Directory containing files that your job will run in. Can be a "
        "local directory or a remote URI to a .zip file (S3, GS, HTTP). "
        "This overrides the working_dir in --runtime-env if both are "
        "specified. This will be passed to ray.init() as the default for "
        "deployments."
    ),
)
@click.option(
    "--app-dir",
    "-d",
    default=".",
    type=str,
    help=APP_DIR_HELP_STR,
)
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", None),
    required=False,
    type=str,
    help=RAY_INIT_ADDRESS_HELP_STR,
)
@click.option(
    "--host",
    "-h",
    default=DEFAULT_HTTP_HOST,
    required=False,
    type=str,
    help=f"Host for HTTP server to listen on. Defaults to {DEFAULT_HTTP_HOST}.",
)
@click.option(
    "--port",
    "-p",
    default=DEFAULT_HTTP_PORT,
    required=False,
    type=int,
    help=f"Port for HTTP servers to listen on. Defaults to {DEFAULT_HTTP_PORT}.",
)
@click.option(
    "--blocking/--non-blocking",
    default=True,
    help=(
        "Whether or not this command should be blocking. If blocking, it "
        "will loop and log status until Ctrl-C'd, then clean up the app."
    ),
)
def run(
    config_or_import_path: str,
    runtime_env: str,
    runtime_env_json: str,
    working_dir: str,
    app_dir: str,
    address: str,
    host: str,
    port: int,
    blocking: bool,
):
    sys.path.insert(0, app_dir)

    final_runtime_env = parse_runtime_env_args(
        runtime_env=runtime_env,
        runtime_env_json=runtime_env_json,
        working_dir=working_dir,
    )

    if pathlib.Path(config_or_import_path).is_file():
        config_path = config_or_import_path
        cli_logger.print(f'Deploying from config file: "{config_path}".')

        with open(config_path, "r") as config_file:
            config = ServeApplicationSchema.parse_obj(yaml.safe_load(config_file))
        is_config = True
    else:
        import_path = config_or_import_path
        cli_logger.print(f'Deploying from import path: "{import_path}".')
        node = import_attr(import_path)
        is_config = False

    # Setting the runtime_env here will set defaults for the deployments.
    ray.init(address=address, namespace=SERVE_NAMESPACE, runtime_env=final_runtime_env)
    client = serve.start(detached=True)

    try:
        if is_config:
            client.deploy_app(config)
        else:
            serve.run(node, host=host, port=port)
        cli_logger.success("Deployed successfully.")

        if blocking:
            while True:
                # Block, letting Ray print logs to the terminal.
                time.sleep(10)

    except KeyboardInterrupt:
        cli_logger.info("Got KeyboardInterrupt, shutting down...")
        serve.shutdown()
        sys.exit()


@cli.command(help="Get the current config of the running Serve app.")
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "http://localhost:8265"),
    required=False,
    type=str,
    help=RAY_DASHBOARD_ADDRESS_HELP_STR,
)
def config(address: str):

    app_info = ServeSubmissionClient(address).get_info()
    if app_info is not None:
        print(yaml.safe_dump(app_info, sort_keys=False))


@cli.command(
    short_help="Get the current status of the running Serve app.",
    help=(
        "Prints status information about all deployments in the Serve app.\n\n"
        "Deployments may be:\n\n"
        "- HEALTHY: all replicas are acting normally and passing their "
        "health checks.\n\n"
        "- UNHEALTHY: at least one replica is not acting normally and may not be "
        "passing its health check.\n\n"
        "- UPDATING: the deployment is updating."
    ),
)
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "http://localhost:8265"),
    required=False,
    type=str,
    help=RAY_DASHBOARD_ADDRESS_HELP_STR,
)
def status(address: str):
    app_status = ServeSubmissionClient(address).get_status()
    if app_status is not None:
        print(yaml.safe_dump(app_status, default_flow_style=False, sort_keys=False))


@cli.command(
    help="Deletes the Serve app.",
)
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "http://localhost:8265"),
    required=False,
    type=str,
    help=RAY_DASHBOARD_ADDRESS_HELP_STR,
)
@click.option("--yes", "-y", is_flag=True, help="Bypass confirmation prompt.")
def shutdown(address: str, yes: bool):
    if not yes:
        click.confirm(
            f"\nThis will shutdown the Serve application at address "
            f'"{address}" and delete all deployments there. Do you '
            "want to continue?",
            abort=True,
        )

    ServeSubmissionClient(address).delete_application()

    cli_logger.newline()
    cli_logger.success("\nSent delete request successfully!\n")
    cli_logger.newline()


@cli.command(
    short_help="Writes a Serve Deployment Graph's config file.",
    help=(
        "Imports the ClassNode or FunctionNode at IMPORT_PATH "
        "and generates a structured config for it that can be used by "
        "`serve deploy` or the REST API. "
    ),
)
@click.argument("import_path")
@click.option(
    "--app-dir",
    "-d",
    default=".",
    type=str,
    help=APP_DIR_HELP_STR,
)
@click.option(
    "--output-path",
    "-o",
    default=None,
    type=str,
    help=(
        "Local path where the output config will be written in YAML format. "
        "If not provided, the config will be printed to STDOUT."
    ),
)
def build(import_path: str, app_dir: str, output_path: Optional[str]):
    sys.path.insert(0, app_dir)

    node: Union[ClassNode, FunctionNode] = import_attr(import_path)
    if not isinstance(node, (ClassNode, FunctionNode)):
        raise TypeError(
            f"Expected '{import_path}' to be ClassNode or "
            f"FunctionNode, but got {type(node)}."
        )

    app = build_app(node)

    config = ServeApplicationSchema(
        deployments=[deployment_to_schema(d) for d in app.deployments.values()]
    ).dict()
    config["import_path"] = import_path

    if output_path is not None:
        if not output_path.endswith(".yaml"):
            raise ValueError("FILE_PATH must end with '.yaml'.")

        with open(output_path, "w") as f:
            yaml.safe_dump(config, stream=f, default_flow_style=False, sort_keys=False)
    else:
        print(yaml.safe_dump(config, default_flow_style=False, sort_keys=False), end="")
