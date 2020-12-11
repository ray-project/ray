#!/usr/bin/env python

import click

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
def cli(address):
    ray.init(address=address)


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
def start(http_host, http_port):
    serve.start(detached=True, http_host=http_host, http_port=http_port)


@cli.command(help="Shutdown the running Serve instance on the Ray cluster.")
def shutdown():
    serve.connect().shutdown()
