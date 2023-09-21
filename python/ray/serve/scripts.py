#!/usr/bin/env python
import os
import pathlib
import sys
import time
from dataclasses import asdict
from typing import Dict, List, Optional, Tuple

import click
import yaml
import traceback
import re
import watchfiles
from pydantic import ValidationError

import ray
from ray import serve
from ray._private.utils import import_attr
from ray.autoscaler._private.cli_logger import cli_logger
from ray.dashboard.modules.dashboard_sdk import parse_runtime_env_args
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.serve.api import build as build_app
from ray.serve.config import DeploymentMode, ProxyLocation, gRPCOptions
from ray.serve._private.constants import (
    DEFAULT_GRPC_PORT,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    SERVE_NAMESPACE,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve._private.common import ServeDeployMode
from ray.serve.deployment import Application, deployment_to_schema
from ray.serve._private import api as _private_api
from ray.serve.schema import (
    ServeApplicationSchema,
    ServeDeploySchema,
    ServeInstanceDetails,
)

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
        if isinstance(v, list):
            data[k] = [process_dict_for_yaml_dump(item) for item in v]
        elif isinstance(v, str):
            data[k] = remove_ansi_escape_sequences(v)

    return data


def convert_args_to_dict(args: Tuple[str]) -> Dict[str, str]:
    args_dict = dict()
    for arg in args:
        split = arg.split("=")
        if len(split) != 2:
            raise click.ClickException(
                f"Invalid application argument '{arg}', "
                "must be of the form '<key>=<val>'."
            )

        args_dict[split[0]] = split[1]

    return args_dict


@click.group(
    help="CLI for managing Serve applications on a Ray cluster.",
    context_settings=dict(help_option_names=["--help", "-h"]),
)
def cli():
    pass


@cli.command(help="Start Serve on the Ray cluster.")
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
    help="Host for HTTP proxies to listen on. " f"Defaults to {DEFAULT_HTTP_HOST}.",
)
@click.option(
    "--http-port",
    default=DEFAULT_HTTP_PORT,
    required=False,
    type=int,
    help="Port for HTTP proxies to listen on. " f"Defaults to {DEFAULT_HTTP_PORT}.",
)
@click.option(
    "--http-location",
    default=DeploymentMode.HeadOnly,
    required=False,
    type=click.Choice(list(DeploymentMode)),
    help="DEPRECATED: Use `--proxy-location` instead.",
)
@click.option(
    "--proxy-location",
    default=ProxyLocation.EveryNode,
    required=False,
    type=click.Choice(list(ProxyLocation)),
    help="Location of the proxies. Defaults to EveryNode.",
)
@click.option(
    "--grpc-port",
    default=DEFAULT_GRPC_PORT,
    required=False,
    type=int,
    help="Port for gRPC proxies to listen on. " f"Defaults to {DEFAULT_GRPC_PORT}.",
)
@click.option(
    "--grpc-servicer-functions",
    default=[],
    required=False,
    multiple=True,
    help="Servicer function for adding the method handler to the gRPC server. "
    "Defaults to an empty list and no gRPC server is started.",
)
def start(
    address,
    http_host,
    http_port,
    http_location,
    proxy_location,
    grpc_port,
    grpc_servicer_functions,
):
    if http_location != DeploymentMode.HeadOnly:
        cli_logger.warning(
            "The `--http-location` flag to `serve start` is deprecated, "
            "use `--proxy-location` instead."
        )

        proxy_location = http_location

    ray.init(
        address=address,
        namespace=SERVE_NAMESPACE,
    )
    serve.start(
        detached=True,
        proxy_location=proxy_location,
        http_options=dict(
            host=http_host,
            port=http_port,
        ),
        grpc_options=gRPCOptions(
            port=grpc_port,
            grpc_servicer_functions=grpc_servicer_functions,
        ),
    )


@cli.command(
    short_help="Deploy Serve application(s) from a YAML config file.",
    help=(
        "This supports both configs of the format ServeApplicationSchema, which "
        "deploys a single application, as well as ServeDeploySchema, which deploys "
        "multiple applications.\n\n"
        "This call is async; a successful response only indicates that the "
        "request was sent to the Ray cluster successfully. It does not mean "
        "the the deployments have been deployed/updated.\n\n"
        "Existing deployments with no code changes will not be redeployed.\n\n"
        "Use `serve config` to fetch the current config(s) and `serve status` to "
        "check the status of the application(s) and deployments after deploying."
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

    try:
        ServeDeploySchema.parse_obj(config)
        ServeSubmissionClient(address).deploy_applications(config)
    except ValidationError as v2_err:
        try:
            ServeApplicationSchema.parse_obj(config)
            ServeSubmissionClient(address).deploy_application(config)
        except ValidationError as v1_err:
            # If we find the field "applications" in the config, most likely
            # user is trying to deploy a multi-application config
            if "applications" in config:
                raise v2_err from None
            else:
                raise v1_err from None
        except RuntimeError as e:
            # Error deploying application
            raise e from None
    except RuntimeError:
        # Error deploying application
        raise

    cli_logger.success(
        "\nSent deploy request successfully.\n "
        "* Use `serve status` to check applications' statuses.\n "
        "* Use `serve config` to see the current application config(s).\n"
    )


@cli.command(
    short_help="Run Serve application(s).",
    help=(
        "Runs an application from the specified import path (e.g., my_script:"
        "app) or application(s) from a YAML config.\n\n"
        "If passing an import path, it must point to a Serve Application or "
        "a function that returns one. If a function is used, arguments can be "
        "passed to it in 'key=val' format after the import path, for example:\n\n"
        "serve run my_script:app model_path='/path/to/model.pkl' num_replicas=5\n\n"
        "If passing a YAML config, existing applications with no code changes will not "
        "be updated.\n\n"
        "By default, this will block and stream logs to the console. If you "
        "Ctrl-C the command, it will shut down Serve on the cluster."
    ),
)
@click.argument("config_or_import_path")
@click.argument("arguments", nargs=-1, required=False)
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
        "Directory containing files that your application(s) will run in. Can be a "
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
    required=False,
    type=str,
    help=f"Host for HTTP server to listen on. Defaults to {DEFAULT_HTTP_HOST}.",
)
@click.option(
    "--port",
    "-p",
    required=False,
    type=int,
    help=f"Port for HTTP proxies to listen on. Defaults to {DEFAULT_HTTP_PORT}.",
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
    help=(
        "Whether to enable gradio visualization of deployment graph. The "
        "visualization can only be used with deployment graphs with DAGDriver "
        "as the ingress deployment."
    ),
)
@click.option(
    "--reload",
    "-r",
    is_flag=True,
    help=(
        "Listens for changes to files in the working directory, --working-dir "
        "or the working_dir in the --runtime-env, and automatically redeploys "
        "the application. This will block until Ctrl-C'd, then clean up the "
        "app."
    ),
)
def run(
    config_or_import_path: str,
    arguments: Tuple[str],
    runtime_env: str,
    runtime_env_json: str,
    working_dir: str,
    app_dir: str,
    address: str,
    host: str,
    port: int,
    blocking: bool,
    gradio: bool,
    reload: bool,
):
    if host is not None or port is not None:
        cli_logger.warning(
            "Specifying `--host` and `--port` to `serve run` is deprecated and will be "
            "removed in a future version. To specify custom HTTP options, use the "
            "`serve start` command."
        )

    sys.path.insert(0, app_dir)
    args_dict = convert_args_to_dict(arguments)
    final_runtime_env = parse_runtime_env_args(
        runtime_env=runtime_env,
        runtime_env_json=runtime_env_json,
        working_dir=working_dir,
    )

    if pathlib.Path(config_or_import_path).is_file():
        if len(args_dict) > 0:
            cli_logger.warning(
                "Application arguments are ignored when running a config file."
            )

        is_config = True
        config_path = config_or_import_path
        cli_logger.print(f"Running config file: '{config_path}'.")

        with open(config_path, "r") as config_file:
            config_dict = yaml.safe_load(config_file)

            try:
                config = ServeDeploySchema.parse_obj(config_dict)
                if gradio:
                    raise click.ClickException(
                        "The gradio visualization feature of `serve run` does not yet "
                        "have support for multiple applications."
                    )

                # If host or port is specified as a CLI argument, they should take
                # priority over config values.
                if host is None:
                    if "http_options" in config_dict:
                        host = config_dict["http_options"].get(
                            "host", DEFAULT_HTTP_HOST
                        )
                    else:
                        host = DEFAULT_HTTP_HOST
                if port is None:
                    if "http_options" in config_dict:
                        port = config_dict["http_options"].get(
                            "port", DEFAULT_HTTP_PORT
                        )
                    else:
                        port = DEFAULT_HTTP_PORT
            except ValidationError as v2_err:
                try:
                    config = ServeApplicationSchema.parse_obj(config_dict)
                    # If host or port is specified as a CLI argument, they should take
                    # priority over config values.
                    if host is None:
                        host = config_dict.get("host", DEFAULT_HTTP_HOST)
                    if port is None:
                        port = config_dict.get("port", DEFAULT_HTTP_PORT)
                except ValidationError as v1_err:
                    # If we find the field "applications" in the config, most likely
                    # user is trying to deploy a multi-application config
                    if "applications" in config_dict:
                        raise v2_err from None
                    else:
                        raise v1_err from None

    else:
        is_config = False
        if host is None:
            host = DEFAULT_HTTP_HOST
        if port is None:
            port = DEFAULT_HTTP_PORT
        import_path = config_or_import_path
        cli_logger.print(f"Running import path: '{import_path}'.")
        app = _private_api.call_app_builder_with_args_if_necessary(
            import_attr(import_path), args_dict
        )

    # Only initialize ray if it has not happened yet.
    if not ray.is_initialized():
        # Setting the runtime_env here will set defaults for the deployments.
        ray.init(
            address=address, namespace=SERVE_NAMESPACE, runtime_env=final_runtime_env
        )
    elif (
        address is not None
        and address != "auto"
        and address != ray.get_runtime_context().gcs_address
    ):
        # Warning users the address they passed is different from the existing ray
        # instance.
        ray_address = ray.get_runtime_context().gcs_address
        cli_logger.warning(
            "An address was passed to `serve run` but the imported module also "
            f"connected to Ray at a different address: '{ray_address}'. You do not "
            "need to call `ray.init` in your code when using `serve run`."
        )

    http_options = {"host": host, "port": port, "location": "EveryNode"}
    grpc_options = gRPCOptions()
    # Merge http_options and grpc_options with the ones on ServeDeploySchema. If host
    # and/or port is passed by cli, those continue to take the priority
    if is_config and isinstance(config, ServeDeploySchema):
        config_http_options = config.http_options.dict()
        http_options = {**config_http_options, **http_options}
        grpc_options = gRPCOptions(**config.grpc_options.dict())

    client = _private_api.serve_start(
        detached=True,
        http_options=http_options,
        grpc_options=grpc_options,
    )

    try:
        if is_config:
            client.deploy_apps(config, _blocking=gradio)
            cli_logger.success("Submitted deploy config successfully.")
            if gradio:
                handle = serve.get_deployment("DAGDriver").get_handle()
        else:
            handle = serve.run(app, host=host, port=port)
            cli_logger.success("Deployed Serve app successfully.")

        if gradio:
            from ray.serve.experimental.gradio_visualize_graph import GraphVisualizer

            visualizer = GraphVisualizer()
            visualizer.visualize_with_gradio(handle)
        elif reload:
            if not blocking:
                raise click.ClickException(
                    "The --non-blocking option conflicts with the --reload option."
                )
            if working_dir:
                watch_dir = working_dir
            else:
                watch_dir = app_dir

            for changes in watchfiles.watch(
                watch_dir,
                rust_timeout=10000,
                yield_on_timeout=True,
            ):
                if changes:
                    cli_logger.info(
                        f"Detected file change in path {watch_dir}. Redeploying app."
                    )
                    # The module needs to be reloaded with `importlib` in order to pick
                    # up any changes.
                    app = _private_api.call_app_builder_with_args_if_necessary(
                        import_attr(import_path, reload_module=True), args_dict
                    )
                    serve.run(app, host=host, port=port)

        if blocking:
            while True:
                # Block, letting Ray print logs to the terminal.
                time.sleep(10)

    except KeyboardInterrupt:
        cli_logger.info("Got KeyboardInterrupt, shutting down...")
        serve.shutdown()
        sys.exit()

    except Exception:
        traceback.print_exc()
        cli_logger.error(
            "Received unexpected error, see console logs for more details. Shutting "
            "down..."
        )
        serve.shutdown()
        sys.exit()


@cli.command(help="Gets the current config(s) of Serve application(s) on the cluster.")
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_AGENT_ADDRESS", "http://localhost:52365"),
    required=False,
    type=str,
    help=RAY_DASHBOARD_ADDRESS_HELP_STR,
)
@click.option(
    "--name",
    "-n",
    required=False,
    type=str,
    help=(
        "Name of an application. Only applies to multi-application mode. If set, this "
        "will only fetch the config for the specified application."
    ),
)
def config(address: str, name: Optional[str]):
    serve_details = ServeInstanceDetails(
        **ServeSubmissionClient(address).get_serve_details()
    )

    if serve_details.deploy_mode != ServeDeployMode.MULTI_APP:
        if name is not None:
            raise click.ClickException(
                "A single-app config was deployed to this cluster, so fetching an "
                "application config by name is not allowed."
            )
        name = SERVE_DEFAULT_APP_NAME

    # Fetch app configs for all live applications on the cluster
    if name is None:
        print(
            "\n---\n\n".join(
                yaml.safe_dump(
                    app.deployed_app_config.dict(exclude_unset=True),
                    sort_keys=False,
                )
                for app in serve_details.applications.values()
                if app.deployed_app_config is not None
            ),
            end="",
        )
    # Fetch a specific app config by name.
    else:
        app = serve_details.applications.get(name)
        if app is None or app.deployed_app_config is None:
            print(f'No config has been deployed for application "{name}".')
        else:
            config = app.deployed_app_config.dict(exclude_unset=True)
            print(yaml.safe_dump(config, sort_keys=False), end="")


@cli.command(
    short_help="Get the current status of all Serve applications on the cluster.",
    help=(
        "Prints status information about all applications on the cluster.\n\n"
        "An application may be:\n\n"
        "- NOT_STARTED: the application does not exist.\n"
        "- DEPLOYING: the deployments in the application are still deploying and "
        "haven't reached the target number of replicas.\n"
        "- RUNNING: all deployments are healthy.\n"
        "- DEPLOY_FAILED: the application failed to deploy or reach a running state.\n"
        "- DELETING: the application is being deleted, and the deployments in the "
        "application are being teared down.\n\n"
        "The deployments within each application may be:\n\n"
        "- HEALTHY: all replicas are acting normally and passing their health checks.\n"
        "- UNHEALTHY: at least one replica is not acting normally and may not be "
        "passing its health check.\n"
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
@click.option(
    "--name",
    "-n",
    default=None,
    required=False,
    type=str,
    help=(
        "Name of an application. If set, this will display only the status of the "
        "specified application."
    ),
)
def status(address: str, name: Optional[str]):
    serve_details = ServeInstanceDetails(
        **ServeSubmissionClient(address).get_serve_details()
    )
    status = asdict(serve_details._get_status())

    # Ensure multi-line strings in app_status is dumped/printed correctly
    yaml.SafeDumper.add_representer(str, str_presenter)

    if name is None:
        print(
            yaml.safe_dump(
                # Ensure exception traceback in app_status are printed correctly
                process_dict_for_yaml_dump(status),
                default_flow_style=False,
                sort_keys=False,
            ),
            end="",
        )
    else:
        if name not in serve_details.applications:
            cli_logger.error(f'Application "{name}" does not exist.')
        else:
            print(
                yaml.safe_dump(
                    # Ensure exception tracebacks in app_status are printed correctly
                    process_dict_for_yaml_dump(status["applications"][name]),
                    default_flow_style=False,
                    sort_keys=False,
                ),
                end="",
            )


@cli.command(
    help="Shuts down Serve on the cluster, deleting all applications.",
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
            f"This will shut down Serve on the cluster at address "
            f'"{address}" and delete all applications there. Do you '
            "want to continue?",
            abort=True,
        )

    ServeSubmissionClient(address).delete_application()

    cli_logger.success(
        "Sent shutdown request; applications will be deleted asynchronously."
    )


@cli.command(
    short_help="Generate a config file for the specified applications.",
    help=(
        "Imports the applications at IMPORT_PATHS and generates a structured, multi-"
        "application config for them. If the flag --single-app is set, accepts one "
        "application and generates a single-application config. Config "
        "outputted from this command can be used by `serve deploy` or the REST API. "
    ),
)
@click.argument("import_paths", nargs=-1, required=True)
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
    help=(
        "Print a single-application Serve config in Kubernetes format. Must be used "
        "with the flag `--single-app`."
    ),
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
@click.option(
    "--multi-app",
    is_flag=True,
    help="Generate a multi-application config from multiple targets.",
)
@click.option(
    "--single-app",
    is_flag=True,
    help="Generate a single-application config from one target.",
)
@click.option(
    "--grpc-servicer-functions",
    default=[],
    required=False,
    multiple=True,
    help="Servicer function for adding the method handler to the gRPC server. "
    "Defaults to an empty list and no gRPC server is started.",
)
def build(
    import_paths: Tuple[str],
    app_dir: str,
    kubernetes_format: bool,
    output_path: Optional[str],
    # This is no longer used, it is only kept here to avoid breaking existing CLI usage
    multi_app: bool,
    single_app: bool,
    grpc_servicer_functions: List[str],
):
    # Add logger messages for users who are still using --multi-app
    if multi_app:
        cli_logger.warning(
            "`serve build` now generates a config in multi-application format by "
            "default. The flag `--multi-app` is now redundant and will be removed soon."
        )
        if single_app:
            raise click.ClickException(
                "You cannot specify both `--single-app` and `--multi-app`."
            )

    sys.path.insert(0, app_dir)

    def build_app_config(import_path: str, _kubernetes_format: bool, name: str = None):
        app: Application = import_attr(import_path)
        if not isinstance(app, Application):
            raise TypeError(
                f"Expected '{import_path}' to be an Application but got {type(app)}."
            )

        app = build_app(app, name)
        schema = ServeApplicationSchema(
            import_path=import_path,
            runtime_env={},
            deployments=[
                deployment_to_schema(d, single_app) for d in app.deployments.values()
            ],
        )
        # If building a multi-app config, auto-generate names for each application.
        # Also, each ServeApplicationSchema should not have host and port set, it should
        # be set at the top level of ServeDeploySchema.
        if single_app:
            schema.host = "0.0.0.0"
            schema.port = 8000
        else:
            schema.name = name
            schema.route_prefix = app.ingress.route_prefix

        if _kubernetes_format:
            return schema.kubernetes_dict(exclude_unset=True)
        else:
            return schema.dict(exclude_unset=True)

    config_str = (
        "# This file was generated using the `serve build` command "
        f"on Ray v{ray.__version__}.\n\n"
    )

    if single_app:
        if len(import_paths) > 1:
            raise click.ClickException(
                "Got more than one argument. Only one import path is accepted when "
                "using the flag `--single-app`."
            )

        config_str += yaml.dump(
            build_app_config(
                import_paths[0], kubernetes_format, SERVE_DEFAULT_APP_NAME
            ),
            Dumper=ServeApplicationSchemaDumper,
            default_flow_style=False,
            sort_keys=False,
        )
    else:
        if kubernetes_format:
            raise click.ClickException(
                "Multi-application config does not support Kubernetes format."
            )

        app_configs = []
        for app_index, import_path in enumerate(import_paths):
            app_configs.append(
                build_app_config(import_path, False, f"app{app_index + 1}")
            )

        deploy_config = {
            "proxy_location": "EveryNode",
            "http_options": {
                "host": "0.0.0.0",
                "port": 8000,
            },
            "grpc_options": {
                "port": DEFAULT_GRPC_PORT,
                "grpc_servicer_functions": grpc_servicer_functions,
            },
            "applications": app_configs,
        }

        # Parse + validate the set of application configs
        ServeDeploySchema.parse_obj(deploy_config)

        config_str += yaml.dump(
            deploy_config,
            Dumper=ServeDeploySchemaDumper,
            default_flow_style=False,
            sort_keys=False,
        )
        cli_logger.info(
            "The auto-generated application names default to `app1`, `app2`, ... etc. "
            "Rename as necessary.\n",
        )

    # Ensure file ends with only one newline
    config_str = config_str.rstrip("\n") + "\n"

    with open(output_path, "w") if output_path else sys.stdout as f:
        f.write(config_str)


class ServeApplicationSchemaDumper(yaml.SafeDumper):
    """YAML dumper object with custom formatting for ServeApplicationSchema.

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

        # Indents must be at most 2 to ensure that only the top 2 levels of
        # the config file have line breaks between them. The top 2 levels include
        # import_path, runtime_env, deployments, and all entries of deployments.
        if len(self.indents) <= 2:
            super().write_line_break()


class ServeDeploySchemaDumper(yaml.SafeDumper):
    """YAML dumper object with custom formatting for ServeDeploySchema.

    Reformat config to follow this spacing:
    ---------------------------------------

    host: 0.0.0.0

    port: 8000

    applications:

    - name: app1

      import_path: app1.path

      runtime_env: {}

      deployments:

      - name: deployment1
        ...

      - name: deployment2
        ...
    """

    def write_line_break(self, data=None):
        # https://github.com/yaml/pyyaml/issues/127#issuecomment-525800484
        super().write_line_break(data)

        # Indents must be at most 4 to ensure that only the top 4 levels of
        # the config file have line breaks between them.
        if len(self.indents) <= 4:
            super().write_line_break()
