import click
import json
import yaml

from typing import Union

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


def format_print(data: dict, indentation: int = 1):
    for k, v in data.items():
        tabs = "".join(["\t" for _ in range(indentation)])
        print(f"{tabs}{k}:", end="")
        if isinstance(v, dict):
            print()
            format_print(v, indentation=indentation + 1)
        else:
            print(f" {v}")


def print_state_api_output(state_data: Union[dict, list], format: str, resource: str):
    if len(state_data) == 0:
        print(f"No {resource} in the cluster")

    if format == "default":
        print(yaml.dump(state_data, indent=4, explicit_start=True))
    elif format == "json":
        print(json.dumps(state_data))
    elif format == "table":
        raise NotImplementedError("Table formatter is not implemented yet.")
    else:
        raise ValueError(
            f"Unexpected format: {format}. "
            "Supported formatting: [default | json | table]"
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
@click.option(
    "--format", default="default", type=click.Choice(["default", "json", "table"])
)
@click.pass_context
def actors(ctx, format: str):
    url = ctx.obj["api_server_url"]
    print_state_api_output(list_actors(api_server_url=url), format, "actors")


@list_state_cli_group.command()
@click.option(
    "--format", default="default", type=click.Choice(["default", "json", "table"])
)
@click.pass_context
def placement_groups(ctx, format: str):
    url = ctx.obj["api_server_url"]
    print_state_api_output(
        list_placement_groups(api_server_url=url),
        format,
        "placement groups",
    )


@list_state_cli_group.command()
@click.option(
    "--format", default="default", type=click.Choice(["default", "json", "table"])
)
@click.pass_context
def nodes(ctx, format: str):
    url = ctx.obj["api_server_url"]
    print_state_api_output(list_nodes(api_server_url=url), format, "nodes")


@list_state_cli_group.command()
@click.option(
    "--format", default="default", type=click.Choice(["default", "json", "table"])
)
@click.pass_context
def jobs(ctx, format: str):
    url = ctx.obj["api_server_url"]
    print_state_api_output(list_jobs(api_server_url=url), format, "jobs")


@list_state_cli_group.command()
@click.option(
    "--format", default="default", type=click.Choice(["default", "json", "table"])
)
@click.pass_context
def workers(ctx, format: str):
    url = ctx.obj["api_server_url"]
    print_state_api_output(list_workers(api_server_url=url), format, "workers")


@list_state_cli_group.command()
@click.option(
    "--format", default="default", type=click.Choice(["default", "json", "table"])
)
@click.pass_context
def tasks(ctx, format: str):
    url = ctx.obj["api_server_url"]
    print_state_api_output(list_tasks(api_server_url=url), format, "tasks")


@list_state_cli_group.command()
@click.option(
    "--format", default="default", type=click.Choice(["default", "json", "table"])
)
@click.pass_context
def objects(ctx, format: str):
    url = ctx.obj["api_server_url"]
    print_state_api_output(list_objects(api_server_url=url), format, "objects")


@list_state_cli_group.command()
@click.option(
    "--format", default="default", type=click.Choice(["default", "json", "table"])
)
@click.pass_context
def runtime_envs(ctx, format: str):
    url = ctx.obj["api_server_url"]
    print_state_api_output(
        list_runtime_envs(api_server_url=url),
        format,
        "runtime envs",
    )
