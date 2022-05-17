from pprint import pprint
from collections import defaultdict

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
    summary_resources,
)


def _get_api_server_url():
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
    return api_server_url


@click.group("list")
@click.pass_context
def list_state_cli_group(ctx):
    api_server_url = _get_api_server_url()
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


@click.group("summary")
@click.pass_context
def summary_state_cli_group(ctx):
    api_server_url = _get_api_server_url()
    assert use_gcs_for_bootstrap()
    ctx.ensure_object(dict)
    ctx.obj["api_server_url"] = f"http://{api_server_url.decode()}"


@summary_state_cli_group.command()
@click.option(
    "-n", "--per-node", is_flag=True, help="Return the per-node resource usage."
)
@click.option(
    "-d",
    "--detail",
    is_flag=True,
    help=(
        "Return the detailed resource usage. E.g., It also displays a "
        "list of tasks and actors that use resources."
    ),
)
@click.pass_context
def resources(ctx, per_node, detail):
    # TODO(sang): Use Pydantic or gRPC protobuf instead of raw dictionary
    # once we decide what to standardize on.
    # Converting nested dict to dataclass is difficult and require a separate
    # library.
    url = ctx.obj["api_server_url"]
    result = summary_resources(per_node=per_node, detail=detail, api_server_url=url)

    usage = {}
    if per_node:
        for node_id, data in result.items():
            usage[node_id] = data
    else:
        usage["cluster"] = result

    # Parse and print it.
    for node_id, data in usage.items():
        print(f"Node ID: {node_id}")
        if detail:
            summary = data["summary"]
        else:
            summary = data

        resource_usage = defaultdict(dict)
        for resource, available in summary["available"].items():
            resource_usage[resource]["available"] = available
        for resource, total in summary["total"].items():
            resource_usage[resource]["total"] = total
        for resource, resource_usage in resource_usage.items():
            if "available" not in resource_usage:
                resource_usage["available"] = 0
            print(
                f"  {resource_usage['total'] - resource_usage['available']}"
                f"/{resource_usage['total']} {resource}"
            )
        print()

        if detail:
            for resource_detail in data["usage"].values():
                task_or_actor_name = resource_detail["task_name"]
                resource_set_list = resource_detail["resource_set_list"]
                print(f"  {task_or_actor_name}, ")
                for resource_set in resource_set_list:
                    print(
                        f"    {resource_set['resource_set']} * {resource_set['count']}"
                    )
