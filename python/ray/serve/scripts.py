#!/usr/bin/env python
import json
import yaml
import os
import sys
import pathlib
import click
import time
from typing import Tuple, List, Dict
import argparse

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
from ray.dashboard.modules.dashboard_sdk import parse_runtime_env_args
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.autoscaler._private.cli_logger import cli_logger


def process_args_and_kwargs(
    args_and_kwargs: Tuple[str],
) -> Tuple[List[str], Dict[str, str]]:
    """
    Takes in a Tuple of strings. Any string prepended with "--" is considered a
    keyword. Keywords must be formatted as --keyword=value or --keyword value.
    All other strings are considered args. All args must come before kwargs.

    For example:

    ("argval1", "argval2", "--kwarg1", "kwval1", "--kwarg2", "kwval2",)

    becomes

    args = ["argval1", "argval2"]
    kwargs = {"kwarg1": "kwval1", "kwarg2": "kwval2"}
    """

    if args_and_kwargs is None:
        return [], {}

    class ErroringArgumentParser(argparse.ArgumentParser):
        """
        ArgumentParser prints and exits upon error. This subclass raises a
        ValueError instead.
        """

        def error(self, message):
            if message.find("unrecognized arguments") == 0:
                # Give clear message when args come between or after kwargs
                arg = message[message.find(":") + 2 :]
                raise ValueError(
                    f'Argument "{arg}" was separated from other args by '
                    "keyword arguments. Args cannot be separated by "
                    f"kwargs.\nMessage from parser: {message}"
                )
            elif message.endswith("expected one argument"):
                # Give clear message when kwargs are undefined
                kwarg = message[message.find("--") : message.rfind(":")]
                raise ValueError(
                    f'Got no value for argument "{kwarg}". All '
                    "keyword arguments specified must have a value."
                    f"\nMessage from parser: {message}"
                )
            else:
                # Raise argparse's error otherwise
                raise ValueError(message)

    parser = ErroringArgumentParser()
    parser.add_argument("args", nargs="*")
    for arg_or_kwarg in args_and_kwargs:
        if arg_or_kwarg[:2] == "--":
            parser.add_argument(arg_or_kwarg.split("=")[0])

    args_and_kwargs = vars(parser.parse_args(args_and_kwargs))
    args = args_and_kwargs["args"]
    del args_and_kwargs["args"]
    return args, args_and_kwargs


def configure_runtime_env(deployment: Deployment, updates: Dict):
    """
    Overwrites deployment's runtime_env with fields in updates. Any fields in
    deployment's runtime_env that aren't in updates stay the same.
    """

    if deployment.ray_actor_options is None:
        deployment._ray_actor_options = {"runtime_env": updates}
    elif "runtime_env" in deployment.ray_actor_options:
        deployment.ray_actor_options["runtime_env"].update(updates)
    else:
        deployment.ray_actor_options["runtime_env"] = updates


@click.group(help="[EXPERIMENTAL] CLI for managing Serve instances on a Ray cluster.")
def cli():
    pass


@cli.command(help="Start a detached Serve instance on the Ray cluster.")
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "auto"),
    required=False,
    type=str,
    help='Address of the running Ray cluster to connect to. Defaults to "auto".',
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
    namespace,
    runtime_env_json,
    http_host,
    http_port,
    http_location,
    checkpoint_path,
):
    ray.init(
        address=address,
        namespace=namespace,
        runtime_env=json.loads(runtime_env_json),
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


@cli.command(help="Shutdown the running Serve instance on the Ray cluster.")
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "auto"),
    required=False,
    type=str,
    help='Address of the running Ray cluster to connect to. Defaults to "auto".',
)
@click.option(
    "--namespace",
    "-n",
    default="serve",
    required=False,
    type=str,
    help='Ray namespace to connect to. Defaults to "serve".',
)
def shutdown(address: str, namespace: str):
    ray.init(
        address=address,
        namespace=namespace,
    )
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
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "auto"),
    required=False,
    type=str,
    help='Address of the running Ray cluster to connect to. Defaults to "auto".',
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
@click.option(
    "--options-json",
    default=r"{}",
    required=False,
    type=str,
    help="JSON string for the deployments options",
)
def create_deployment(
    address: str,
    namespace: str,
    runtime_env_json: str,
    deployment: str,
    options_json: str,
):
    ray.init(
        address=address,
        namespace=namespace,
        runtime_env=json.loads(runtime_env_json),
    )
    deployment_cls = import_attr(deployment)
    if not isinstance(deployment_cls, Deployment):
        deployment_cls = serve.deployment(deployment_cls)
    options = json.loads(options_json)
    deployment_cls.options(**options).deploy()


@cli.command(
    short_help="[Experimental] Deploy deployments from a YAML config file.",
    help=(
        "Deploys deployment(s) from CONFIG_OR_IMPORT_PATH, which must be either a "
        "Serve YAML configuration file path or an import path to "
        "a class or function to deploy.\n\n"
        "Import paths must be of the form "
        '"module.submodule_1...submodule_n.MyClassOrFunction".\n\n'
        "Sends a nonblocking request. A successful response only indicates that the "
        "request was received successfully. It does not mean the deployments are "
        "live. Use `serve info` and `serve status` to check on them. "
    ),
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

    with open(config_file_name, "r") as config_file:
        config = yaml.safe_load(config_file)

    # Schematize config to validate format
    ServeApplicationSchema.parse_obj(config)

    ServeSubmissionClient(address).deploy_application(config)

    cli_logger.newline()
    cli_logger.success(
        "\nSent deploy request successfully!\n "
        "* Use `serve status` to check your deployments' statuses.\n "
        "* Use `serve info` to see your running Serve "
        "application's configuration.\n"
    )
    cli_logger.newline()


@cli.command(
    short_help="[Experimental] Run deployments via Serve's Python API.",
    help=(
        "Deploys deployment(s) from CONFIG_OR_IMPORT_PATH, which must be either a "
        "Serve YAML configuration file path or an import path to "
        "a class or function to deploy.\n\n"
        "The full command must be of the form:\n"
        '"serve run [import path] [optional parameters] -- [arg-1] ... [arg-n] '
        '[kwarg-1]=[kwval-1] ... [kwarg-n]=[kwval-n]"\n\n'
        "Deployments via import path may also take in init_args and "
        "init_kwargs from any ARGS_AND_KWARGS passed in. Import paths must be "
        "of the form:\n"
        '"module.submodule_1...submodule_n.MyClassOrFunction".\n\n'
        "Blocks after deploying, and logs status periodically. After being killed, "
        "this command tears down all deployments it deployed. If there are no "
        "deployments left, it also tears down the Serve application."
    ),
)
@click.argument("config_or_import_path")
@click.argument("args_and_kwargs", required=False, nargs=-1)
@click.option(
    "--runtime-env",
    type=str,
    default=None,
    required=False,
    help="Path to a local YAML file containing a runtime_env definition. "
    "Overrides all runtime_envs specified in a config file.",
)
@click.option(
    "--runtime-env-json",
    type=str,
    default=None,
    required=False,
    help="JSON-serialized runtime_env dictionary. Overrides all runtime_envs "
    "specified in a config file.",
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
        "specified. Overrides all working_dirs specified in a config file."
    ),
)
@click.option(
    "--cluster-address",
    "-c",
    default="auto",
    required=False,
    type=str,
    help=('Address of the Ray cluster to query. Defaults to "auto".'),
)
@click.option(
    "--dashboard-address",
    "-d",
    default="http://localhost:8265",
    required=False,
    type=str,
    help=(
        'Address of the Ray dashboard to query. Defaults to "http://localhost:8265".'
    ),
)
def run(
    config_or_import_path: str,
    args_and_kwargs: Tuple[str],
    runtime_env: str,
    runtime_env_json: str,
    working_dir: str,
    cluster_address: str,
    dashboard_address: str,
):

    try:
        # Check if path provided is for config or import
        deployments = []
        is_config = pathlib.Path(config_or_import_path).is_file()
        args, kwargs = process_args_and_kwargs(args_and_kwargs)
        runtime_env_updates = parse_runtime_env_args(
            runtime_env=runtime_env,
            runtime_env_json=runtime_env_json,
            working_dir=working_dir,
        )

        if is_config:
            config_path = config_or_import_path
            # Delay serve.start() to catch invalid inputs without waiting
            if len(args) + len(kwargs) > 0:
                raise ValueError(
                    "ARGS_AND_KWARGS cannot be defined for a "
                    "config file deployment. Please specify the "
                    "init_args and init_kwargs inside the config file."
                )

            cli_logger.print(
                "Deploying application in config file at " f"{config_path}."
            )
            with open(config_path, "r") as config_file:
                config = yaml.safe_load(config_file)

            schematized_config = ServeApplicationSchema.parse_obj(config)
            deployments = schema_to_serve_application(schematized_config)

            ray.init(address=cluster_address, namespace="serve")
            serve.start()
            ServeSubmissionClient(dashboard_address)._upload_working_dir_if_needed(
                runtime_env_updates
            )

            for deployment in deployments:
                configure_runtime_env(deployment, runtime_env_updates)
            deploy_group(deployments)

            cli_logger.newline()
            cli_logger.success(
                f'\nDeployments from config file at "{config_path}" '
                "deployed successfully!\n"
            )
            cli_logger.newline()

        else:
            import_path = config_or_import_path
            cli_logger.print(
                f'Deploying function or class imported from "{import_path}".'
            )

            if "." not in import_path:
                raise ValueError(
                    "Import paths must be of the form "
                    '"module.submodule_1...submodule_n.MyClassOrFunction".'
                )
            deployment_name = import_path[import_path.rfind(".") + 1 :]
            deployment = serve.deployment(name=deployment_name)(import_path)
            deployments = [deployment]

            ray.init(address=cluster_address, namespace="serve")
            serve.start()
            ServeSubmissionClient(dashboard_address)._upload_working_dir_if_needed(
                runtime_env_updates
            )

            configure_runtime_env(deployment, runtime_env_updates)
            deployment.options(
                init_args=args,
                init_kwargs=kwargs,
            ).deploy()

            cli_logger.newline()
            cli_logger.print(f"\nDeployed import at {import_path} successfully!\n")
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
        cli_logger.print("Got SIGINT (KeyboardInterrupt). Removing deployments.")
        for deployment in deployments:
            deployment.delete()
        if len(serve.list_deployments()) == 0:
            cli_logger.print("No deployments left. Shutting down Serve.")
            serve.shutdown()
        sys.exit()


@cli.command(
    short_help="[Experimental] Get info about your Serve application's config.",
    help=(
        "Prints the configurations of all running deployments in the Serve "
        "application."
    ),
)
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "http://localhost:8265"),
    required=False,
    type=str,
    help='Address of the Ray dashboard to query. For example, "http://localhost:8265".',
)
@click.option(
    "--json_format",
    "-j",
    is_flag=True,
    help="Print info as json. If omitted, info is printed as YAML.",
)
def info(address: str, json_format=bool):

    app_info = ServeSubmissionClient(address).get_info()
    if app_info is not None:
        if json_format:
            print(json.dumps(app_info, indent=4))
        else:
            print(yaml.dump(app_info))


@cli.command(
    short_help="[Experimental] Get your Serve application's status.",
    help=(
        "Prints status information about all deployments in the Serve application.\n\n"
        "Deployments may be:\n\n"
        "- HEALTHY: all replicas are acting normally and passing their health checks.\n"
        "- UNHEALTHY: at least one replica is not acting normally and may not be "
        "passing its health check.\n"
        "- UPDATING: the deployment is updating."
    ),
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

    app_status = ServeSubmissionClient(address).get_status()
    if app_status is not None:
        print(json.dumps(app_status, indent=4))


@cli.command(
    short_help=(
        "[EXPERIMENTAL] Deletes all running deployments in the Serve application."
    ),
    help="Deletes all running deployments in the Serve application.",
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

    ServeSubmissionClient(address).delete_application()

    cli_logger.newline()
    cli_logger.success("\nSent delete request successfully!\n")
    cli_logger.newline()
