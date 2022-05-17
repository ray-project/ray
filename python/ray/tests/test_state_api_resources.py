import time
import ray
import requests
import pytest

from ray.dashboard.tests.conftest import *  # noqa
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
)




# @ray.remote(num_cpus=1, memory=1000, resources={"custom_a": 1})
# def f():
#     while True:
#         time.sleep(10)


# @ray.remote(num_cpus=1, num_gpus=1)
# def g():
#     while True:
#         time.sleep(10)


# @ray.remote(num_cpus=1, resources={"custom_b": 1})
# class Actor:
#     pass


# @pytest.mark.parametrize(
#     "ray_start_cluster", [{"include_dashboard": True}], indirect=True
# )
# def test_resource_summary_and_usage(ray_start_cluster):
#     cluster = ray_start_cluster
#     cluster.add_node(num_cpus=32, num_gpus=4, resources={"custom_a": 16})
#     cluster.add_node(num_cpus=32, resources={"custom_b": 16})

#     context = ray.init(address=cluster.address)
#     assert wait_until_server_available(context["webui_url"]) is True
#     webui_url = context["webui_url"]
#     webui_url = format_web_url(webui_url)

#     gs = [g.remote() for _ in range(4)]
#     actors = [Actor.remote() for _ in range(16)]
#     # Make sure tasks requiring special resources have been placed
#     time.sleep(1)
#     fs = [f.remote() for _ in range(4)]
#     fs_2 = [
#         f.options(num_cpus=1, memory=1000, resources={"custom_a": 2}).remote()
#         for _ in range(4)
#     ]
#     time.sleep(1)

#     resources_url_path = webui_url + "/api/v0/resources"

#     def get_check_and_parse(path: str):
#         response = requests.get(
#             resources_url_path + path, headers={"Content-Type": "application/json"}
#         )
#         response.raise_for_status()
#         json_response = response.json()
#         if not json_response["result"]:
#             raise ValueError(json_response["msg"])
#         return json_response["data"]["result"]

#     # Check cluster total resources
#     cluster_resources_summary = get_check_and_parse("/summary/cluster")

#     cluster_resources_total = cluster_resources_summary["total"]

#     assert cluster_resources_total["custom_b"] == 16
#     assert cluster_resources_total["custom_a"] == 16
#     assert cluster_resources_total["GPU"] == 4
#     assert cluster_resources_total["CPU"] == 64

#     # Check cluster available resources
#     cluster_resources_available = cluster_resources_summary["available"]

#     assert "custom_b" not in cluster_resources_available
#     assert "GPU" not in cluster_resources_available
#     assert cluster_resources_available["custom_a"] == 4.0
#     assert cluster_resources_available["CPU"] == 64 - 8 - 4 - 16

#     # Check nodes total resources
#     nodes_resources_summary = get_check_and_parse("/summary/nodes")

#     assert len(nodes_resources_summary) == 2
#     for node_id, resources in nodes_resources_summary.items():
#         resources = resources["total"]
#         if "custom_a" in resources:
#             node_a_id = node_id
#             assert resources["CPU"] == 32.0
#             assert resources["GPU"] == 4.0
#             assert resources["custom_a"] == 16.0
#             assert "custom_b" not in resources
#         else:
#             assert resources["CPU"] == 32.0
#             assert resources["custom_b"] == 16.0
#             assert "GPU" not in resources
#             assert "custom_a" not in resources

#     # Check nodes available resources
#     assert len(nodes_resources_summary) == 2
#     for node_id, resources in nodes_resources_summary.items():
#         resources = resources["available"]
#         if node_id == node_a_id:
#             assert resources["custom_a"] == 4.0
#             assert resources["CPU"] == 32 - 12
#             assert "GPU" not in resources
#             assert "custom_b" not in resources
#         else:
#             assert "custom_a" not in resources
#             assert resources["CPU"] == 32 - 16
#             assert "GPU" not in resources
#             assert "custom_b" not in resources

#     # Wait for nodes to be updated in API server
#     time.sleep(5)

#     # Check cluster resource usage
#     cluster_resource_detailed = get_check_and_parse("/usage/cluster")

#     cluster_resource_detailed_summary = cluster_resource_detailed["summary"]
#     cluster_resource_usage = cluster_resource_detailed["usage"]

#     assert cluster_resources_summary == cluster_resource_detailed_summary

#     f_resources = [
#         {
#             "resource_set": {"memory": 1000.0, "CPU": 1.0, "custom_a": 2.0},
#             "count": 4,
#         },
#         {
#             "resource_set": {"memory": 1000.0, "CPU": 1.0, "custom_a": 1.0},
#             "count": 4,
#         },
#     ]
#     g_resources = [{"resource_set": {"CPU": 1.0, "GPU": 1.0}, "count": 4}]
#     actor_resources = [{"resource_set": {"CPU": 1.0, "custom_b": 1.0}, "count": 16}]

#     assert cluster_resource_usage["g"]["resource_set_list"] == g_resources

#     assert len(cluster_resource_usage["f"]["resource_set_list"]) == 2

#     assert all(
#         [r in cluster_resource_usage["f"]["resource_set_list"] for r in f_resources]
#     )

#     assert cluster_resource_usage["Actor"]["resource_set_list"] == actor_resources

#     # Check nodes resource usage
#     nodes_resource_detailed = get_check_and_parse("/usage/nodes")

#     assert len(nodes_resource_detailed) == 2
#     for node_id, resources in nodes_resource_detailed.items():
#         resources_summary = resources["summary"]
#         resources = resources["usage"]

#         assert resources_summary == nodes_resources_summary[node_id]

#         if node_id == node_a_id:
#             assert resources["g"]["resource_set_list"] == g_resources

#             assert len(resources["f"]["resource_set_list"]) == 2
#             assert all([r in resources["f"]["resource_set_list"] for r in f_resources])
#             assert "Actor" not in resources
#         else:
#             assert resources["Actor"]["resource_set_list"] == actor_resources
#             assert "f" not in resources
#             assert "g" not in resources

#     del gs, actors, fs, fs_2


# @ray.remote(num_cpus=1)
# def h():
#     while True:
#         time.sleep(10)


# @ray.remote(num_cpus=1, resources={"custom_nested": 1})
# def nested_function():
#     ray.get(h.remote())


# @pytest.mark.parametrize(
#     "ray_start_cluster", [{"include_dashboard": True}], indirect=True
# )
# def test_resource_blocked_consumes_no_resources(ray_start_cluster):
#     cluster = ray_start_cluster
#     cluster.add_node(num_cpus=32, resources={"custom_nested": 16})

#     context = ray.init(address=cluster.address)
#     assert wait_until_server_available(context["webui_url"]) is True
#     webui_url = context["webui_url"]
#     webui_url = format_web_url(webui_url)

#     nested_fns = [nested_function.remote() for _ in range(16)]
#     time.sleep(5)

#     resources_url_path = webui_url + "/api/v0/resources"

#     def get_check_and_parse(path: str):
#         response = requests.get(
#             resources_url_path + path, headers={"Content-Type": "application/json"}
#         )
#         response.raise_for_status()
#         json_response = response.json()
#         if not json_response["result"]:
#             raise ValueError(json_response["msg"])
#         return json_response["data"]["result"]

#     # Check cluster total resources
#     cluster_resources_summary = get_check_and_parse("/summary/cluster")

#     cluster_resources_total = cluster_resources_summary["total"]

#     assert cluster_resources_total["custom_nested"] == 16
#     assert cluster_resources_total["CPU"] == 32

#     # Check cluster available resources
#     cluster_resources_available = cluster_resources_summary["available"]

#     assert cluster_resources_total["custom_nested"] == 16
#     assert cluster_resources_available["CPU"] == 16

#     # Check cluster resource usage
#     cluster_resource_detailed = get_check_and_parse("/usage/cluster")

#     cluster_resource_detailed_summary = cluster_resource_detailed["summary"]
#     cluster_resource_usage = cluster_resource_detailed["usage"]

#     assert cluster_resources_summary == cluster_resource_detailed_summary

#     h_resources = [
#         {
#             "resource_set": {"CPU": 1.0},
#             "count": 16,
#         }
#     ]
#     assert "nested" not in cluster_resource_usage
#     assert cluster_resource_usage["h"]["resource_set_list"] == h_resources

#     del nested_fns
