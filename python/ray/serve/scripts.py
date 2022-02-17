#!/usr/bin/env python
import json
import os
from readline import redisplay

import click

import ray
from ray.serve.api import Deployment
from ray.serve.config import DeploymentMode
from ray._private.utils import import_attr
from ray import serve
from ray.serve.constants import (
    DEFAULT_CHECKPOINT_PATH,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
)
from ray.dashboard.modules.common import SubmissionClient


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
    help=(
        "Runtime environment dictionary to pass into ray.init. " "Defaults to empty."
    ),
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
def deploy_path(deployment: str, options_json: str):
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
@click.argument("config_fname")
@click.option(
    "--address",
    "-a",
    default="http://localhost:8265",
    required=False,
    type=str,
    help="Address of the Ray dashboard to query.",
)
def deploy(config_fname: str, address: str):
    import yaml
    import requests

    full_address = f"{address}/api/serve/deployments/"

    with open(config_fname, "r") as config:
        deployments = yaml.safe_load(config)["deployments"]

    submission_client = SubmissionClient(address)
    for deployment in deployments:
        # Refomat deployment dictionary into REST API format
        configurables = deployment["configurable"]
        del deployment["configurable"]
        deployment.update(configurables)
        for key, val in deployment.items():
            if isinstance(val, str) and val.lower() == "none":
                deployment[key] = None

        # Upload local working_dir if provided
        ray_actor_options = deployment.get("ray_actor_options", None)
        if ray_actor_options is not None:
            runtime_env = ray_actor_options.get("runtime_env", None)
            if runtime_env is not None:
                submission_client._upload_working_dir_if_needed(runtime_env)

    response = requests.put(full_address, json=deployments)
    if response.status_code == 200:
        print("Deployment succeeded!")
    else:
        print("Deployment failed!")


@cli.command(
    help="[Experimental] Run YAML configuration file via Serve's Python API.",
    hidden=True,
)
@click.argument("config_fname")
@click.option(
    "--address",
    "-a",
    default=os.environ.get("RAY_ADDRESS"),
    required=False,
    type=str,
    help="Address of the running Ray cluster to connect to. " 'Defaults to "auto".',
)
def run(config_fname: str, address: str):
    from ray.serve.api import deploy_group
    import yaml
    import time

    with open(config_fname, "r") as config:
        deployment_data_list = yaml.safe_load(config)["deployments"]

    deployments = []
    for deployment_data in deployment_data_list:
        configurables = deployment_data["configurable"]
        del deployment_data["configurable"]
        deployment_data.update(configurables)

        import_path = deployment_data["import_path"]
        del deployment_data["import_path"]

        for key in list(deployment_data.keys()):
            val = deployment_data[key]
            if isinstance(val, str) and val.lower() == "none":
                del deployment_data[key]

        deployments.append(serve.deployment(**deployment_data)(import_path))

    deploy_group(deployments)
    print("Group deployed successfully!")

    while True:
        time.sleep(100)


@cli.command(
    help="[Experimental] Get info about deployments in your Serve cluster.",
    hidden=True,
)
@click.option(
    "--address",
    "-a",
    default="http://localhost:8265/api/serve/deployments/",
    required=False,
    type=str,
    help="Address of the Ray dashboard to query.",
)
def info(address: str):
    import requests

    response = requests.get(address)
    if response.status_code == 200:
        print("Serve instance info:\n")
        deployments = response.json()
        for name, status in deployments.items():
            print(f'Deployment "{name}": ')
            print(status, "\n")
    else:
        print("Failed to acquire serve instance info!")
