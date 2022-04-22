import time
import ray
import requests
import pytest
import json

from ray.dashboard.tests.conftest import *  # noqa
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
)


@ray.remote(num_cpus=1, memory=1000, resources={"custom_a": 1})
def f():
    while True:
        time.sleep(10)


@ray.remote(num_cpus=1, num_gpus=1)
def g():
    while True:
        time.sleep(10)


@ray.remote(num_cpus=1, resources={"custom_b": 1})
class Actor:
    pass


@pytest.mark.parametrize(
    "ray_start_cluster", [{"include_dashboard": True}], indirect=True
)
def test_all(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=32, num_gpus=4, resources={"custom_a": 16})
    cluster.add_node(num_cpus=32, resources={"custom_b": 16})

    context = ray.init(address=cluster.address)
    assert wait_until_server_available(context["webui_url"]) is True
    webui_url = context["webui_url"]
    webui_url = format_web_url(webui_url)

    gs = [g.remote() for _ in range(4)]
    actors = [Actor.remote() for _ in range(16)]
    # Make sure tasks requiring special resources have been placed
    time.sleep(1)
    fs = [f.remote() for _ in range(4)]
    fs_2 = [
        f.options(num_cpus=1, memory=1000, resources={"custom_a": 2}).remote()
        for _ in range(4)
    ]
    time.sleep(1)

    resources_url_path = webui_url + "/api/experimental/resources"

    def get_check_and_parse(path: str):
        response = requests.get(resources_url_path + path)
        if response.status_code != 200:
            raise ValueError("Failed query HTTP endpoint")
        print("resources: ", path)
        print(response.text)
        print("")
        return json.loads(response.text)

    # Check cluster total resources
    cluster_resources_summary = get_check_and_parse("/summary/cluster")

    cluster_resources_total = cluster_resources_summary["total"]

    assert cluster_resources_total["custom_b"] == 16
    assert cluster_resources_total["custom_a"] == 16
    assert cluster_resources_total["GPU"] == 4
    assert cluster_resources_total["CPU"] == 64

    # Check cluster available resources
    cluster_resources_available = cluster_resources_summary["available"]

    assert "custom_b" not in cluster_resources_available
    assert "GPU" not in cluster_resources_available
    assert cluster_resources_available["custom_a"] == 4.0
    assert cluster_resources_available["CPU"] == 64 - 8 - 4 - 16

    # Check nodes total resources
    nodes_resources_summary = get_check_and_parse("/summary/nodes")
    nodes_resources_total = nodes_resources_summary["total"]

    assert len(nodes_resources_total) == 2
    for node_id, resources in nodes_resources_total.items():
        if "custom_a" in resources:
            node_a_id = node_id
            assert resources["CPU"] == 32.0
            assert resources["GPU"] == 4.0
            assert resources["custom_a"] == 16.0
            assert "custom_b" not in resources
        else:
            assert resources["CPU"] == 32.0
            assert resources["custom_b"] == 16.0
            assert "GPU" not in resources
            assert "custom_a" not in resources

    # Check nodes available resources
    nodes_resources_available = nodes_resources_summary["available"]

    assert len(nodes_resources_available) == 2
    for node_id, resources in nodes_resources_available.items():
        if node_id == node_a_id:
            assert resources["custom_a"] == 4.0
            assert resources["CPU"] == 32 - 12
            assert "GPU" not in resources
            assert "custom_b" not in resources
        else:
            assert "custom_a" not in resources
            assert resources["CPU"] == 32 - 16
            assert "GPU" not in resources
            assert "custom_b" not in resources

    # Wait for nodes to be updated in API server
    time.sleep(5)

    # Check cluster resource usage
    cluster_resource_usage = get_check_and_parse("/usage/cluster")

    assert cluster_resource_usage["g"] == [
        {"resource_set": {"CPU": 1.0, "GPU": 1.0}, "count": 4}
    ]

    assert len(cluster_resource_usage["f"]) == 2
    f_resources = [
        {
            "resource_set": {"memory": 1000.0, "CPU": 1.0, "custom_a": 2.0},
            "count": 4,
        },
        {
            "resource_set": {"memory": 1000.0, "CPU": 1.0, "custom_a": 1.0},
            "count": 4,
        },
    ]
    assert all([r in cluster_resource_usage["f"] for r in f_resources])

    assert cluster_resource_usage["Actor"] == [
        {"resource_set": {"CPU": 1.0, "custom_b": 1.0}, "count": 16}
    ]

    # Check nodes resource usage
    nodes_resource_usage = get_check_and_parse("/usage/nodes")

    assert len(nodes_resource_usage) == 2
    for node_id, resources in nodes_resource_usage.items():
        if node_id == node_a_id:
            assert resources["g"] == [
                {"resource_set": {"CPU": 1.0, "GPU": 1.0}, "count": 4}
            ]

            assert len(resources["f"]) == 2
            f_resources = [
                {
                    "resource_set": {"memory": 1000.0, "CPU": 1.0, "custom_a": 2.0},
                    "count": 4,
                },
                {
                    "resource_set": {"memory": 1000.0, "CPU": 1.0, "custom_a": 1.0},
                    "count": 4,
                },
            ]
            assert all([r in resources["f"] for r in f_resources])
            assert "Actor" not in resources
        else:
            assert resources["Actor"] == [
                {"resource_set": {"CPU": 1.0, "custom_b": 1.0}, "count": 16}
            ]
            assert "f" not in resources
            assert "g" not in resources

    del gs, actors, fs, fs_2
