#!/usr/bin/env python
import json
import yaml
import os
import pathlib
import click
from typing import Tuple, List, Dict
import argparse

import ray
from ray.serve.config import DeploymentMode
from ray import serve
from ray.serve.constants import (
    DEFAULT_CHECKPOINT_PATH,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
)
from ray.serve.schema import ServeApplicationSchema
from ray.dashboard.modules.dashboard_sdk import parse_runtime_env_args
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.autoscaler._private.cli_logger import cli_logger
from ray.serve.application import Application
from ray.serve.api import Deployment

RAY_INIT_ADDRESS_HELP_STR = (
    "Address to use for ray.init(). Can also be specified "
    "using the RAY_ADDRESS environment variable."
)
RAY_DASHBOARD_ADDRESS_HELP_STR = (
    "Address to use to query the Ray dashboard (defaults to "
    "http://localhost:8265). Can also be specified using the "
    "RAY_ADDRESS environment variable."
)


def _process_args_and_kwargs(
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


def _configure_runtime_env(deployment: Deployment, updates: Dict):
    """Overwrites deployment's runtime_env with fields in updates.

    Any fields in deployment's runtime_env that aren't in updates stay the
    same.
    """

    if deployment.ray_actor_options is None:
        deployment._ray_actor_options = {"runtime_env": updates}
    else:
        current_env = deployment.ray_actor_options.get("runtime_env", {})
        updates.update(current_env)
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
    help=RAY_INIT_ADDRESS_HELP_STR,
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
    http_host,
    http_port,
    http_location,
    checkpoint_path,
):
    ray.init(
        address=address,
        namespace=namespace,
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


@cli.command(help="Shut down the running Serve instance on the Ray cluster.")
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "auto"),
    required=False,
    type=str,
    help=RAY_INIT_ADDRESS_HELP_STR,
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
    short_help="Deploy a Serve app from a YAML config file.",
    help=(
        "Deploys deployment(s) from a YAML config file.\n\n"
        "This call is async; a successful response only indicates that the "
        "request was sent to the Ray cluster successfully. It does not mean "
        "the the deployments have been deployed/updated.\n\n"
        "Use `serve info` to fetch the current config and `serve status` to "
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
    short_help="Run a Serve app in a blocking way.",
    help=(
        "Runs the Serve app from the specified YAML config file or import path "
        "for a deployment class or function (`my_module.MyClassOrFunction`).\n"
        "If deploying via import path, you can pass args and kwargs to the "
        "constructor using serve run -- [arg1]...[argn] "
        "[--kwarg1=val1]...[--kwargn=valn].\n"
        "Blocks after deploying and logs status periodically. If you Ctrl-C "
        "this command, it tears down the app."
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
    "Overrides runtime_envs specified in the config file.",
)
@click.option(
    "--runtime-env-json",
    type=str,
    default=None,
    required=False,
    help="JSON-serialized runtime_env dictionary. Overrides runtime_envs "
    "specified in the config file.",
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
        "specified. Overrides working_dirs specified in the config file."
    ),
)
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", None),
    required=False,
    type=str,
    help=RAY_INIT_ADDRESS_HELP_STR,
)
def run(
    config_or_import_path: str,
    args_and_kwargs: Tuple[str],
    runtime_env: str,
    runtime_env_json: str,
    working_dir: str,
    address: str,
):
    # Check if path provided is for config or import
    is_config = pathlib.Path(config_or_import_path).is_file()
    args, kwargs = _process_args_and_kwargs(args_and_kwargs)

    # Calculate deployments' runtime env updates requested via args
    runtime_env_updates = parse_runtime_env_args(
        runtime_env=runtime_env,
        runtime_env_json=runtime_env_json,
        working_dir=working_dir,
    )

    # Create ray.init()'s runtime_env
    if "working_dir" in runtime_env_updates:
        ray_runtime_env = {"working_dir": runtime_env_updates.pop("working_dir")}
    else:
        ray_runtime_env = {}

    if is_config:
        config_path = config_or_import_path
        # Delay serve.start() to catch invalid inputs without waiting
        if len(args) + len(kwargs) > 0:
            raise ValueError(
                "ARGS_AND_KWARGS cannot be defined for a "
                "config file deployment. Please specify the "
                "init_args and init_kwargs inside the config file."
            )

        cli_logger.print("Deploying application in config file at " f"{config_path}.")
        with open(config_path, "r") as config_file:
            app = Application.from_yaml(config_file)

    else:
        import_path = config_or_import_path
        if "." not in import_path:
            raise ValueError(
                "Import paths must be of the form "
                '"module.submodule_1...submodule_n.MyClassOrFunction".'
            )

        cli_logger.print(f'Deploying function or class imported from "{import_path}".')

        deployment_name = import_path[import_path.rfind(".") + 1 :]
        deployment = serve.deployment(name=deployment_name)(import_path)

        app = Application([deployment.options(init_args=args, init_kwargs=kwargs)])

    ray.init(address=address, namespace="serve", runtime_env=ray_runtime_env)

    for deployment in app:
        _configure_runtime_env(deployment, runtime_env_updates)

    app.run(logger=cli_logger)


@cli.command(
    short_help="Get the current config of the running Serve app.",
    help=("Prints the configurations of all running deployments in the Serve app."),
)
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS", "http://localhost:8265"),
    required=False,
    type=str,
    help=RAY_DASHBOARD_ADDRESS_HELP_STR,
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
    short_help="Get the current status of the Serve app.",
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
        print(json.dumps(app_status, indent=4))


@cli.command(
    short_help=("Deletes all running deployments in the Serve app."),
    help="Deletes all running deployments in the Serve app.",
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
