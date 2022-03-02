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
        return

    keys = ["name", "schedulingState", "requiredResources", "SchedulingStateDetail"]
    from collections import defaultdict

    values = defaultdict(list)
    for d in data:
        if "name" in d:
            if "SchedulingStateDetail" not in d:
                d["SchedulingStateDetail"] = "?"
                d["schedulingState"] = "Running"
            if "requiredResources" not in d:
                d["requiredResources"] = "?"
            for k in keys:
                values[k].append(d[k])
    from tabulate import tabulate

    print(tabulate(values, headers="keys", tablefmt="github"))


@get_state_cli_group.command()
@click.pass_context
def actors(ctx):
    url = ctx.obj["api_server_url"]
    r = requests.request(
        "GET",
        f"{url}/actors/get",
        headers={"Content-Type": "application/json"},
        json=None,
        timeout=10,
    )
    r.raise_for_status()
    actors = r.json()["data"]["actors"]
    if len(actors) == 0:
        print("No actors in the cluster.")
        return

    keys = ["actorClass", "state", "pid", "actorId"]
    from collections import defaultdict

    values = defaultdict(list)
    for actor in actors.values():
        for k in keys:
            values[k].append(actor[k])
    from tabulate import tabulate

    print(tabulate(values, headers="keys", tablefmt="github"))


@get_state_cli_group.command()
@click.pass_context
def objects(ctx):
    url = ctx.obj["api_server_url"]
    r = requests.request(
        "GET",
        f"{url}/objects/get",
        headers={"Content-Type": "application/json"},
        json=None,
        timeout=10,
    )
    r.raise_for_status()
    objects = r.json()["data"]["memory"]
    if len(objects) == 0:
        print("No actors in the cluster.")
        return
    print(objects)


@get_state_cli_group.command()
@click.pass_context
def placement_groups(ctx):
    url = ctx.obj["api_server_url"]
    r = requests.request(
        "GET",
        f"{url}/placement_groups/get",
        headers={"Content-Type": "application/json"},
        json=None,
        timeout=10,
    )
    r.raise_for_status()
    pgs = r.json()["data"]["pgs"]
    if len(pgs) == 0:
        print("No pgs in the cluster.")
        return

    keys = ["placementGroupId", "state", "bundles"]
    from collections import defaultdict

    values = defaultdict(list)
    for pg in pgs.values():
        for k in keys:
            if k == "bundles":
                values[k].append([p["unitResources"] for p in pg[k]])
            else:
                values[k].append(pg[k])
    from tabulate import tabulate

    print(tabulate(values, headers="keys", tablefmt="github"))


@get_state_cli_group.command()
@click.pass_context
def nodes(ctx):
    url = ctx.obj["api_server_url"]
    r = requests.request(
        "GET",
        f"{url}/nodes/get",
        headers={"Content-Type": "application/json"},
        json=None,
        timeout=10,
    )
    r.raise_for_status()
    nodes = r.json()["data"]["nodes"]
    if len(nodes) == 0:
        print("No nodes in the cluster.")
        return

    keys = ["nodeId", "nodeManagerAddress", "state"]
    from collections import defaultdict

    values = defaultdict(list)
    for node in nodes.values():
        for k in keys:
            values[k].append(node[k])
    from tabulate import tabulate

    print(tabulate(values, headers="keys", tablefmt="github"))
