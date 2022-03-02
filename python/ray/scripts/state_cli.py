import click
import ray
import requests

import ray._private.services as services
import ray.ray_constants as ray_constants
from ray.state import GlobalState
from ray._private.gcs_utils import use_gcs_for_bootstrap
from ray._raylet import GcsClientOptions
from ray._private.gcs_utils import GcsClient


@click.group("get")
@click.pass_context
def get_state_cli_group(ctx):
    address = services.canonicalize_bootstrap_address(None)
    gcs_client = GcsClient(address=address, nums_reconnect_retry=0)
    ray.experimental.internal_kv._initialize_internal_kv(gcs_client)
    api_server_url = ray.experimental.internal_kv._internal_kv_get(
        ray_constants.DASHBOARD_ADDRESS,
        namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
    )

    state = GlobalState()
    if use_gcs_for_bootstrap():
        options = GcsClientOptions.from_gcs_address(address)
    else:
        assert False
    state._initialize_global_state(options)
    ctx.ensure_object(dict)
    ctx.obj["state"] = state
    ctx.obj["api_server_url"] = f"http://{api_server_url.decode()}"


@get_state_cli_group.command()
@click.pass_context
def tasks(ctx):
    url = ctx.obj["api_server_url"]
    r = requests.request(
        "GET",
        f"{url}/tasks/get",
        headers={"Content-Type": "application/json"},
        json=None,
        timeout=10,
    )
    r.raise_for_status()
    data = list(r.json()["data"]["tasks"].values())
    if len(data) == 0:
        print("No tasks in the cluster")

    keys = ["name", "schedulingState", "requiredResources", "SchedulingStateDetail"]
    from collections import defaultdict

    values = defaultdict(list)
    for d in data:
        if "name" in d:
            if "SchedulingStateDetail" not in d:
                d["SchedulingStateDetail"] = ""
                d["schedulingState"] = "Running"
            for k in keys:
                values[k].append(d[k])
    from tabulate import tabulate

    print(tabulate(values, headers="keys", tablefmt="github"))

    # print(result["data"]["tasks"])
    # print(tabulate(result["data"]["tasks"].values(), headers="key"))


@get_state_cli_group.command()
@click.pass_context
def actors(ctx):
    state = ctx.obj["state"]
    print(state.actor_table(None))
