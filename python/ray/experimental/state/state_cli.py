from pprint import pprint

import click
import ray

import ray._private.services as services
import ray.ray_constants as ray_constants
from ray._private.gcs_utils import use_gcs_for_bootstrap
from ray._private.gcs_utils import GcsClient

from ray.experimental.state.api import (
    list_actors,
    list_nodes,
    list_jobs,
    list_placement_groups,
    list_workers,
    list_tasks,
    list_objects,
    list_runtime_envs,
)


@click.group("list")
@click.pass_context
def list_state_cli_group(ctx):
    address = services.canonicalize_bootstrap_address(None)
    gcs_client = GcsClient(address=address, nums_reconnect_retry=0)
    ray.experimental.internal_kv._initialize_internal_kv(gcs_client)
    api_server_url = ray._private.utils.internal_kv_get_with_retry(
        gcs_client,
        ray_constants.DASHBOARD_ADDRESS,
        namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
        num_retries=20,
    )
    if api_server_url is None:
        raise ValueError(
            (
                "Couldn't obtain the API server address from GCS. It is likely that "
                "the GCS server is down. Check gcs_server.[out | err] to see if it is "
                "still alive."
            )
        )

    assert use_gcs_for_bootstrap()
    ctx.ensure_object(dict)
    ctx.obj["api_server_url"] = f"http://{api_server_url.decode()}"


@list_state_cli_group.command()
@click.pass_context
def actors(ctx):
    url = ctx.obj["api_server_url"]
    pprint(list_actors(url))


@list_state_cli_group.command()
@click.pass_context
def placement_groups(ctx):
    url = ctx.obj["api_server_url"]
    pprint(list_placement_groups(url))


@list_state_cli_group.command()
@click.pass_context
def nodes(ctx):
    url = ctx.obj["api_server_url"]
    pprint(list_nodes(url))


@list_state_cli_group.command()
@click.pass_context
def jobs(ctx):
    url = ctx.obj["api_server_url"]
    pprint(list_jobs(url))


@list_state_cli_group.command()
@click.pass_context
def workers(ctx):
    url = ctx.obj["api_server_url"]
    pprint(list_workers(url))


@list_state_cli_group.command()
@click.pass_context
def tasks(ctx):
    url = ctx.obj["api_server_url"]
    pprint(list_tasks(url))


@list_state_cli_group.command()
@click.pass_context
def objects(ctx):
    url = ctx.obj["api_server_url"]
    pprint(list_objects(url))


@list_state_cli_group.command()
@click.pass_context
def runtime_envs(ctx):
    url = ctx.obj["api_server_url"]
    pprint(list_runtime_envs(url))
