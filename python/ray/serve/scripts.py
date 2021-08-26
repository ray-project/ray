#!/usr/bin/env python

import click
from ray.serve.config import DeploymentMode

import ray
from ray import serve
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
    ray.init(address=address, namespace=namespace)


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
    serve.api._connect()
    serve.shutdown()
