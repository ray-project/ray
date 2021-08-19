#!/usr/bin/env python

from pprint import pprint
import time

import click

import ray
from ray import serve
from ray.serve.config import DeploymentMode
from ray.serve.constants import DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT


@click.group(
    help="[EXPERIMENTAL] CLI for managing Serve instances on a Ray cluster.")
@click.option(
    "--address",
    "-a",
    default="auto",
    required=False,
    type=str,
    help="Address of the running Ray cluster to connect to. "
    "Defaults to \"auto\".")
@click.option(
    "--namespace",
    "-n",
    default="serve",
    required=False,
    type=str,
    help="Ray namespace to connect to. Defaults to \"serve\".")
def cli(address, namespace):
    try:
        ray.init(address=address, namespace=namespace)
    except ConnectionError:
        raise ConnectionError(
            "Failed to connect to Ray. If you're trying to run locally, "
            "you need to run `ray start --head` first. If you're trying "
            "to connect to a remote cluster, make sure to pass "
            "`--address`.")


@cli.command(help="Start a detached Serve instance on the Ray cluster.")
@click.option(
    "--http-host",
    default=DEFAULT_HTTP_HOST,
    required=False,
    type=str,
    help="Host for HTTP servers to listen on. "
    f"Defaults to {DEFAULT_HTTP_HOST}.")
@click.option(
    "--http-port",
    default=DEFAULT_HTTP_PORT,
    required=False,
    type=int,
    help="Port for HTTP servers to listen on. "
    f"Defaults to {DEFAULT_HTTP_PORT}.")
@click.option(
    "--http-location",
    default=DeploymentMode.HeadOnly,
    required=False,
    type=click.Choice(list(DeploymentMode)),
    help="Location of the HTTP servers. Defaults to HeadOnly.")
def start(http_host, http_port, http_location):
    serve.start(
        detached=True,
        http_options=dict(
            host=http_host,
            port=http_port,
            location=http_location,
        ))


@cli.command(help="Shutdown the running Serve instance on the Ray cluster.")
def shutdown():
    serve.connect().shutdown()


@cli.command(help="Print a list of the deployments currently running.")
def list_deployments():
    pprint.pprint(serve.list_deployments())


@cli.command(help="Deploy a specified deployment.")
@click.option(
    "--name",
    default=None,
    required=False,
    type=str,
)
@click.option(
    "--version",
    default=None,
    required=False,
    type=str,
)
@click.option(
    "--prev-version",
    default=None,
    required=False,
    type=str,
)
@click.option(
    "--num-replicas",
    default=None,
    required=False,
    type=int,
)
@click.option("--block", is_flag=True)
@click.argument("target", type=str)
@click.argument("args", nargs=-1)
def deploy(name: str, version: str, prev_version: str, num_replicas: int,
           target: str, block: bool, args):
    err = ValueError(
        "Target to deploy must be a string of the form <file>:<callable>.")
    if not isinstance(target, str):
        raise err

    split = target.split(":")
    if len(split) != 2:
        raise err

    filename, deployment = split[0], split[1]
    module = __import__(filename)
    deployment = getattr(module, deployment)

    if not isinstance(deployment, serve.api.Deployment):
        deployment = serve.deployment(deployment)

    deployment.options(
        name=name,
        version=version,
        prev_version=prev_version,
        num_replicas=num_replicas).deploy(*args)

    if block:
        while True:
            time.sleep(10)


@cli.command(help="Delete a deployment.")
@click.argument("name", type=str)
def delete(name: str):
    serve.get_deployment(name).delete()
