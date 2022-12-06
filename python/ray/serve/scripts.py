#!/usr/bin/env python
import asyncio
import os
import importlib
import pathlib
import shutil
import signal
import sys
import tempfile
import time
from typing import Optional, Union
from uuid import uuid4

import click
import yaml
import re

import ray
from ray import serve
from ray._private.utils import import_attr, get_module_and_attr
from ray.autoscaler._private.cli_logger import cli_logger
from ray.dashboard.modules.dashboard_sdk import parse_runtime_env_args
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.job_submission import JobSubmissionClient, JobStatus
from ray.serve.api import build as build_app
from ray.serve.config import DeploymentMode
from ray.serve._private.constants import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    SERVE_NAMESPACE,
)
from ray.serve.deployment import deployment_to_schema
from ray.serve.deployment_graph import ClassNode, FunctionNode
from ray.serve.schema import ServeApplicationSchema
from ray.serve._private import api as _private_api

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
    "Address to use to query the Ray dashboard agent (defaults to "
    "http://localhost:52365). Can also be specified using the "
    "RAY_AGENT_ADDRESS environment variable."
)


# See https://stackoverflow.com/a/33300001/11162437
def str_presenter(dumper: yaml.Dumper, data):
    """
    A custom representer to write multi-line strings in block notation using a literal
    style.

    Ensures strings with newline characters print correctly.
    """

    if len(data.splitlines()) > 1:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


# See https://stackoverflow.com/a/14693789/11162437
def remove_ansi_escape_sequences(input: str):
    """Removes ANSI escape sequences in a string"""
    ansi_escape = re.compile(
        r"""
        \x1B  # ESC
        (?:   # 7-bit C1 Fe (except CSI)
            [@-Z\\-_]
        |     # or [ for CSI, followed by a control sequence
            \[
            [0-?]*  # Parameter bytes
            [ -/]*  # Intermediate bytes
            [@-~]   # Final byte
        )
    """,
        re.VERBOSE,
    )

    return ansi_escape.sub("", input)


def process_dict_for_yaml_dump(data):
    """
    Removes ANSI escape sequences recursively for all strings in dict.

    We often need to use yaml.dump() to print dictionaries that contain exception
    tracebacks, which can contain ANSI escape sequences that color printed text. However
    yaml.dump() will format the tracebacks incorrectly if ANSI escape sequences are
    present, so we need to remove them before dumping.
    """

    for k, v in data.items():
        if isinstance(v, dict):
            data[k] = process_dict_for_yaml_dump(v)
        elif isinstance(v, str):
            data[k] = remove_ansi_escape_sequences(v)

    return data


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
def start(address, http_host, http_port, http_location):
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
    )


@cli.command(
    short_help="Deploy a Serve app from a YAML config file.",
    help=(
        "Deploys deployment(s) from a YAML config file.\n\n"
        "This call is async; a successful response only indicates that the "
        "request was sent to the Ray cluster successfully. It does not mean "
        "the the deployments have been deployed/updated.\n\n"
        "Existing deployments with no code changes will not be redeployed.\n\n"
        "Use `serve config` to fetch the current config and `serve status` to "
        "check the status of the deployments after deploying."
    ),
)
@click.argument("config_file_name")
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_AGENT_ADDRESS", "http://localhost:52365"),
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
        "Runs the Serve app from the specified import path (e.g. "
        "my_script:my_bound_deployment) or YAML config.\n\n"
        "If using a YAML config, existing deployments with no code changes "
        "will not be redeployed.\n\n"
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
@click.option(
    "--gradio",
    is_flag=True,
    help=("Whether to enable gradio visualization of deployment graph."),
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
    gradio: bool,
):
    sys.path.insert(0, app_dir)

    # If address is specified through either the command or through the RAY_ADDRESS
    # environment variable, use Ray Job Submission to run serve. Otherwise, connect
    # to local ray clusters.
    if address is None:
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
        ray.init(namespace=SERVE_NAMESPACE, runtime_env=final_runtime_env)

        if is_config:
            client = _private_api.serve_start(
                detached=True,
                http_options={
                    "host": config.host,
                    "port": config.port,
                    "location": "EveryNode",
                },
            )
        else:
            client = _private_api.serve_start(
                detached=True,
                http_options={"host": host, "port": port, "location": "EveryNode"},
            )

        try:
            if is_config:
                client.deploy_app(config, _blocking=gradio)
                cli_logger.success("Submitted deploy config successfully.")
                if gradio:
                    handle = serve.get_deployment("DAGDriver").get_handle()
            else:
                handle = serve.run(node, host=host, port=port)
                cli_logger.success("Deployed Serve app successfully.")

            if gradio:
                from ray.serve.experimental.gradio_visualize_graph import (
                    GraphVisualizer,
                )

                visualizer = GraphVisualizer()
                visualizer.visualize_with_gradio(handle)
            else:
                if blocking:
                    while True:
                        # Block, letting Ray print logs to the terminal.
                        time.sleep(10)

        except KeyboardInterrupt:
            cli_logger.info("Got KeyboardInterrupt, shutting down...")
            serve.shutdown()
            sys.exit()

    else:
        with tempfile.TemporaryDirectory() as tmp_dir:
            # The submitted job needs to access the target file (either the config file
            # or module containing the Serve app). Thus, we need to use working
            # directory in Ray Job Submission to copy over the target file to the
            # remote Ray Cluster.
            #   If user specifies a working directory, use it as is except copy in the
            #   target file, taking a unique name so it doesn't overwrite anything.
            #   If user doesn't specify a working directory, make a temporary directory
            #   that contains only the target file, and pass this temporary directory as
            #   working_dir to the JobSubmissionClient.
            working_dir = working_dir or tmp_dir

            unique_file_name = str(uuid4())
            if pathlib.Path(config_or_import_path).is_file():
                cli_logger.print(
                    f'Deploying from config file: "{config_or_import_path}".'
                )
                config_or_import_path = f"{unique_file_name}.yaml"
                is_config = True

                copy_target = os.path.join(working_dir, f"{unique_file_name}.yaml")
                shutil.copyfile(config_or_import_path, copy_target)
            else:
                cli_logger.print(
                    f'Deploying from import path: "{config_or_import_path}".'
                )
                module, attr_name = get_module_and_attr(config_or_import_path)
                config_or_import_path = f"{unique_file_name}:{attr_name}"
                is_config = False

                copy_target = os.path.join(working_dir, f"{unique_file_name}.py")
                shutil.copyfile(module.__file__, copy_target)

            final_runtime_env = parse_runtime_env_args(
                runtime_env=runtime_env,
                runtime_env_json=runtime_env_json,
                working_dir=working_dir,
            )
            if "env_vars" not in final_runtime_env:
                final_runtime_env["env_vars"] = {}
            final_runtime_env["env_vars"]["RAY_JOB_STOP_SIGNAL"] = "SIGINT"

            script = """
# Ray blocks SIGINT signals, so to properly shutdown serve upon keyboard interrupt,
# need to unblock SIGINT.
import signal
signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})

import argparse
import importlib
import signal
import sys
import time

from ray import serve
from ray._private.utils import import_attr
from ray.autoscaler._private.cli_logger import cli_logger
from ray.serve._private import api as _private_api

parser = argparse.ArgumentParser()
parser.add_argument('--config-or-import-path')
parser.add_argument('--host')
parser.add_argument('--port')
parser.add_argument('--is-config', action='store_true')
parser.add_argument('--blocking', action='store_true')
parser.add_argument('--gradio', action='store_true')
args = parser.parse_args()


if args.is_config:
    config_path = args.config_or_import_path

    with open(config_path, "r") as config_file:
        config = ServeApplicationSchema.parse_obj(yaml.safe_load(config_file))
else:
    import_path = args.config_or_import_path
    node = import_attr(import_path)

if args.is_config:
    client = _private_api.serve_start(
        detached=True,
        http_options={
            'host': config.host,
            'port': config.port,
            'location': 'EveryNode',
        },
    )
else:
    client = _private_api.serve_start(
        detached=True,
        http_options={'host': args.host, 'port': args.port, 'location': 'EveryNode'},
    )

try:
    if args.is_config:
        client.deploy_app(config, _blocking=args.gradio)
        cli_logger.success('Submitted deploy config successfully.')
        if args.gradio:
            handle = serve.get_deployment('DAGDriver').get_handle()
    else:
        handle = serve.run(node, host=args.host, port=args.port)
        cli_logger.success('Deployed Serve app successfully.')

    if args.gradio:
        from ray.serve.experimental.gradio_visualize_graph import GraphVisualizer

        visualizer = GraphVisualizer()
        visualizer.visualize_with_gradio(handle)
    else:
        if args.blocking:
            while True:
                # Block, letting Ray print logs to the terminal.
                time.sleep(10)

except KeyboardInterrupt:
    serve.shutdown()
    sys.exit()
"""

            client = JobSubmissionClient(address)
            submission_id = client.submit_job(
                entrypoint=(
                    f'python -c "{script}" '
                    f"--config-or-import-path={config_or_import_path} "
                    f"--host={host} "
                    f"--port={port} "
                    + ("--is_config " if is_config else "")
                    + ("--blocking " if blocking else "")
                    + ("--gradio " if gradio else "")
                ),
                runtime_env=final_runtime_env,
            )

            async def print_logs():
                async for lines in client.tail_job_logs(submission_id):
                    print(lines, end="")

            def interrupt_handler():
                cli_logger.info("Got KeyboardInterrupt, shutting down...")
                client.stop_job(submission_id)
                # Remove file that was copied into working_dir for the purpose of
                # copying it into the remote Ray Cluster.
                os.remove(copy_target)

            loop = asyncio.get_event_loop()
            loop.add_signal_handler(signal.SIGINT, interrupt_handler)
            loop.run_until_complete(print_logs())


@cli.command(help="Get the current config of the running Serve app.")
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_AGENT_ADDRESS", "http://localhost:52365"),
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
    default=os.environ.get("RAY_AGENT_ADDRESS", "http://localhost:52365"),
    required=False,
    type=str,
    help=RAY_DASHBOARD_ADDRESS_HELP_STR,
)
def status(address: str):
    app_status = ServeSubmissionClient(address).get_status()
    if app_status is not None:
        # Ensure multi-line strings in app_status is dumped/printed correctly
        yaml.SafeDumper.add_representer(str, str_presenter)
        print(
            yaml.safe_dump(
                # Ensure exception tracebacks in app_status are printed correctly
                process_dict_for_yaml_dump(app_status),
                default_flow_style=False,
                sort_keys=False,
            )
        )


@cli.command(
    help="Deletes the Serve app.",
)
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_AGENT_ADDRESS", "http://localhost:52365"),
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
    "--kubernetes_format",
    "-k",
    is_flag=True,
    help="Print Serve config in Kubernetes format.",
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
def build(
    import_path: str, app_dir: str, kubernetes_format: bool, output_path: Optional[str]
):
    sys.path.insert(0, app_dir)

    node: Union[ClassNode, FunctionNode] = import_attr(import_path)
    if not isinstance(node, (ClassNode, FunctionNode)):
        raise TypeError(
            f"Expected '{import_path}' to be ClassNode or "
            f"FunctionNode, but got {type(node)}."
        )

    app = build_app(node)
    schema = ServeApplicationSchema(
        import_path=import_path,
        runtime_env={},
        host="0.0.0.0",
        port=8000,
        deployments=[deployment_to_schema(d) for d in app.deployments.values()],
    )

    if kubernetes_format:
        config = schema.kubernetes_dict(exclude_unset=True)
    else:
        config = schema.dict(exclude_unset=True)

    config_str = (
        "# This file was generated using the `serve build` command "
        f"on Ray v{ray.__version__}.\n\n"
    )
    config_str += yaml.dump(
        config, Dumper=ServeBuildDumper, default_flow_style=False, sort_keys=False
    )

    # Ensure file ends with only one newline
    config_str = config_str.rstrip("\n") + "\n"

    with open(output_path, "w") if output_path else sys.stdout as f:
        f.write(config_str)


class ServeBuildDumper(yaml.SafeDumper):
    """YAML dumper object with custom formatting for `serve build` command.

    Reformat config to follow this spacing:
    ---------------------------------------

    import_path: example.path

    runtime_env: {}

    deployments:

    - name: val1
        ...

    - name: val2
        ...
    """

    def write_line_break(self, data=None):
        # https://github.com/yaml/pyyaml/issues/127#issuecomment-525800484
        super().write_line_break(data)

        # Indents must be less than 3 to ensure that only the top 2 levels of
        # the config file have line breaks between them. The top 2 levels include
        # import_path, runtime_env, deployments, and all entries of deployments.
        if len(self.indents) < 3:
            super().write_line_break()
