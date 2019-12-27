import json

import click

import ray
import ray.experimental.serve as serve


@click.group("serve", help="Commands working with ray serve")
def serve_cli():
    pass


@serve_cli.command(help="Initialize ray serve components")
def init():
    ray.init(address="auto")
    serve.init(blocking=True)


@serve_cli.command(help="Split traffic for a endpoint")
@click.argument("endpoint", required=True, type=str)
# TODO(simon): Make traffic dictionary more ergonomic. e.g.
#     --traffic backend1=0.5 --traffic backend2=0.5
@click.option(
    "--traffic",
    required=True,
    type=str,
    help="Traffic dictionary in JSON format")
def split(endpoint, traffic):
    ray.init(address="auto")
    serve.init()

    serve.split(endpoint, json.loads(traffic))


@serve_cli.command(help="Scale the number of replicas for a backend")
@click.argument("backend", required=True, type=str)
@click.option(
    "--num-replicas",
    required=True,
    type=int,
    help="New number of replicas to set")
def scale(backend_tag, num_replicas):
    if num_replicas <= 0:
        click.Abort(
            "Cannot set number of replicas to be smaller or equal to 0.")
    ray.init(address="auto")
    serve.init()

    serve.scale(backend_tag, num_replicas)
