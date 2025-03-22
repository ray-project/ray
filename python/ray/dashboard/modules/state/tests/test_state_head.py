import sys
import requests

from ray._private.test_utils import format_web_url, wait_until_server_available
from ray.dashboard.tests.conftest import *  # noqa

logger = logging.getLogger(__name__)


def test_fail():
    assert False


def test_list_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    head = cluster.add_node(num_cpus=0)
    webui_url = head.address_info["webui_url"]
    assert wait_until_server_available(webui_url) is True
    webui_url = format_web_url(webui_url)
    url = webui_url + "/api/v0/nodes"

    resp = requests.get(url)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["data"]["result"]["total"] == 1
    assert len(resp_json["data"]["result"]["result"]) == 1

    cluster.add_node(num_cpus=0)
    resp = requests.get(url)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["data"]["result"]["total"] == 2
    assert len(resp_json["data"]["result"]["result"]) == 2


def test_list_tasks(ray_start_with_dashboard):
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    url = webui_url + "/api/v0/tasks"

    resp = requests.get(url)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["data"]["result"]["total"] == 0
    assert len(resp_json["data"]["result"]["result"]) == 0

    @ray.remote
    def f():
        return 1

    ray.get(f.remote())
    resp = requests.get(url)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json == 1
    assert resp_json["data"]["result"]["total"] == 1
    assert len(resp_json["data"]["result"]["result"]) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
