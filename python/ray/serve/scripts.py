#!/usr/bin/env python

import click
import pprint

import ray
from ray import serve
from ray.serve.constants import DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT


@click.group()
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


@cli.command()
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


@cli.command()
def shutdown():
    serve.connect().shutdown()


@cli.command()
def list_endpoints():
    pprint.pprint(serve.connect().list_endpoints())


@cli.command()
def list_backends():
    pprint.pprint(serve.connect().list_backends())
