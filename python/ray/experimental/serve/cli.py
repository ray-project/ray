import json

import click

import ray
import ray.experimental.serve as serve


@click.group()
def serve():
    pass


@serve.command()
@click.argument("endpoint", required=True, type=str)
@click.option("--traffic", required=True, type=str, help="JSON for traffic dictionary")
def split(endpoint, traffic):
    ray.init(address="auto")
    serve.split(endpoint, json.loads(traffic))


@serve.command()
@click.argument("backend", required=True, type=str)
@click.option(
    "--num-replicas", required=True, type=int, help="New number of replicas to set"
)
def scale(backend_tag, num_replicas):
    if num_replicas <= 0:
        click.Abort("Cannot set number of replicas to be smaller or equal to 0.")
    ray.init(address="auto")
    serve.scale(backend_tag, num_replicas)


@serve.command()
@click.option("--percentiles", multiple=True, type=int)
@click.option("--windows", multiple=True, type=int)
def stat(percentiles, windows):
    ray.init(address="auto")
    # TODO(simon): Handle empty values, store default to constants.py
    serve.stat(percentiles, windows)
