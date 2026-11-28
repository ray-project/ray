import collections
import io
import json
import sys
import threading
import time

import pytest
import requests

from ray._private.authentication.http_token_authentication import (
    get_auth_headers_if_auth_enabled,
)
from ray.dashboard.modules.job.actor_manager import (
    _compute_spec_hash,
    _load_user_target,
    _LogTeeStream,
    _normalize_module_source,
    _normalize_spec_resources,
    _validate_resource_num,
)


def test_actor_manager_pure_helpers():
    assert _normalize_module_source(None) is None
    assert (
        _normalize_module_source("\n\nclass Counter:\n    pass\n")
        == "class Counter:\n    pass\n"
    )
    assert _validate_resource_num(2, "num_cpus") == 2
    assert _validate_resource_num(0.5, "num_cpus") == 0.5
    with pytest.raises(ValueError, match="non-negative finite"):
        _validate_resource_num(True, "num_cpus")
    with pytest.raises(ValueError, match="non-negative finite"):
        _validate_resource_num(float("inf"), "num_cpus")
    with pytest.raises(ValueError, match="non-negative finite"):
        _validate_resource_num(-1, "num_cpus")

    norm = _normalize_spec_resources(
        {
            "num_cpus": 2.0,
            "num_gpus": 0,
            "resources": {"CPU": 2.0, "TPU": 4, "zero": 0, "custom": 1.0},
        },
        "class",
    )
    assert norm == (2.0, 0, None, {"TPU": 4.0, "custom": 1.0})

    spec_a = {
        "kind": "class",
        "class_name": "Counter",
        "module_source": "\nclass Counter:\n    pass\n",
        "args": [1],
        "kwargs": {},
        "num_cpus": 1,
        "resources": {"CPU": 1.0, "GPU": 0},
    }
    spec_b = {
        "kind": "class",
        "class_name": "Counter",
        "module_source": "class Counter:\n    pass\n",
        "args": [1],
        "kwargs": {},
        "num_cpus": 1.0,
        "num_gpus": 0.0,
    }
    assert _compute_spec_hash(spec_a, "class", "Counter", 1, 1) == _compute_spec_hash(
        spec_b, "class", "Counter", 1.0, 1
    )

    cls = _load_user_target(
        "test_actor",
        "Counter",
        "class Counter:\n    def __init__(self, v=0):\n        self.v = v\n",
        None,
        {},
    )
    assert cls(5).v == 5

    buf = io.StringIO()
    deq = collections.deque()
    lock = threading.Lock()
    tee = _LogTeeStream(buf, deq, lock)
    tee.write("hello\n")
    tee.flush()
    assert buf.getvalue() == "hello\n"
    assert list(deq) == ["hello\n"]


@pytest.fixture(scope="module")
def dashboard_base_url():
    from ray.tests.conftest import _ray_start

    with _ray_start(include_dashboard=True, num_cpus=4) as ctx:
        url = ctx.address_info["webui_url"]
        if not url.startswith("http"):
            url = f"http://{url}"
        deadline = time.monotonic() + 30.0
        headers = get_auth_headers_if_auth_enabled({})
        while time.monotonic() < deadline:
            try:
                r = requests.get(
                    f"{url}/api/cluster/nodes", headers=headers, timeout=2.0
                )
                if r.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.2)
        yield url, headers


def test_cluster_observability_endpoints(dashboard_base_url):
    base_url, headers = dashboard_base_url
    r_groups = requests.get(
        f"{base_url}/api/cluster/node_groups", headers=headers, timeout=10.0
    )
    assert r_groups.status_code == 200
    groups = r_groups.json()
    assert isinstance(groups, list)
    assert len(groups) >= 1

    r_nodes = requests.get(
        f"{base_url}/api/cluster/nodes", headers=headers, timeout=10.0
    )
    assert r_nodes.status_code == 200
    nodes = r_nodes.json()
    assert isinstance(nodes, list)
    assert len(nodes) >= 1
    assert nodes[0]["state"] == "ALIVE"


def test_function_and_stateful_actor_rest_lifecycle(dashboard_base_url):
    base_url, headers = dashboard_base_url

    # 1. One-shot function execution via POST /api/actors + POST /methods/__call__
    fn_src = (
        "def compute(x, scale=2):\n"
        "    print(f'computing {x} * {scale}')\n"
        "    return {'product': x * scale}\n"
    )
    r_fn_create = requests.post(
        f"{base_url}/api/actors",
        headers=headers,
        json={
            "kind": "function",
            "fn_name": "compute",
            "module_source": fn_src,
        },
        timeout=30.0,
    )
    assert r_fn_create.status_code == 200
    fn_info = r_fn_create.json()
    assert fn_info["ok"] is True
    fn_actor_name = fn_info["name"]

    r_fn_call = requests.post(
        f"{base_url}/api/actors/{fn_actor_name}/methods/__call__",
        headers=headers,
        json={"args": [7], "kwargs": {"scale": 6}},
        timeout=30.0,
    )
    assert r_fn_call.status_code == 200
    body = r_fn_call.json()
    assert body == {"ok": True, "result": {"product": 42}}

    # 2. Named stateful actor creation, get_if_exists idempotence, and recreate
    actor_src = (
        "class Counter:\n"
        "    def __init__(self, start=0):\n"
        "        self.v = start\n"
        "    def add(self, delta=1):\n"
        "        self.v += delta\n"
        "        print(f'counter={self.v}')\n"
        "        return self.v\n"
        "    def stream_range(self, count):\n"
        "        for i in range(count):\n"
        "            yield {'i': i, 'v': self.v + i}\n"
    )
    r_create = requests.post(
        f"{base_url}/api/actors",
        headers=headers,
        json={
            "kind": "class",
            "name": "srv-test-counter",
            "get_if_exists": True,
            "class_name": "Counter",
            "module_source": actor_src,
            "args": [10],
        },
        timeout=30.0,
    )
    assert r_create.status_code == 200
    created_info = r_create.json()
    assert created_info["ok"] is True
    assert created_info["created"] is True
    actor_name = created_info["name"]

    # Re-create with get_if_exists=True reattaches without recreating
    r_same = requests.post(
        f"{base_url}/api/actors",
        headers=headers,
        json={
            "kind": "class",
            "name": "srv-test-counter",
            "get_if_exists": True,
            "class_name": "Counter",
            "module_source": actor_src,
            "args": [10],
        },
        timeout=10.0,
    )
    assert r_same.status_code == 200
    assert r_same.json()["ok"] is True
    assert r_same.json()["created"] is False

    # 3. Invoke method & stream generator method via SSE
    r_inv = requests.post(
        f"{base_url}/api/actors/{actor_name}/methods/add",
        headers=headers,
        json={"args": [5], "kwargs": {}},
        timeout=10.0,
    )
    assert r_inv.status_code == 200
    inv_body = r_inv.json()
    assert inv_body["ok"] is True
    assert inv_body["result"] == 15

    r_stream = requests.post(
        f"{base_url}/api/actors/{actor_name}/methods/stream_range",
        headers=headers,
        json={"args": [3], "kwargs": {}, "stream": True},
        stream=True,
        timeout=10.0,
    )
    assert r_stream.status_code == 200
    events = []
    for raw_line in r_stream.iter_lines(decode_unicode=True):
        if raw_line and raw_line.startswith("data: "):
            events.append(json.loads(raw_line[len("data: ") :]))
    items = [e["item"] for e in events if "item" in e]
    assert items == [{"i": 0, "v": 15}, {"i": 1, "v": 16}, {"i": 2, "v": 17}]

    # 4. Fetch actor logs and delete actor
    r_logs = requests.get(
        f"{base_url}/api/actors/{actor_name}/logs",
        headers=headers,
        timeout=10.0,
    )
    assert r_logs.status_code == 200
    assert "counter=15" in r_logs.json()["logs"]

    r_del = requests.delete(
        f"{base_url}/api/actors/{actor_name}",
        headers=headers,
        timeout=10.0,
    )
    assert r_del.status_code == 200
    assert r_del.json()["killed"] is True


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
