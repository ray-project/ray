import os
import requests
import tempfile
import time

import ray
from ray import serve

def request_with_retries(endpoint, timeout=30):
    start = time.time()
    while True:
        try:
            return requests.get(
                "http://127.0.0.1:8000" + endpoint, timeout=timeout)
        except requests.RequestException:
            if time.time() - start > timeout:
                raise TimeoutError
            time.sleep(0.1)


def _kill_router():
    [router] = ray.get(serve.api._get_master_actor().get_router.remote())
    ray.kill(router)


_, new_db_path = tempfile.mkstemp(suffix=".test.db")
serve.init(
    kv_store_path=new_db_path,
    blocking=True,
    ray_init_kwargs={"num_cpus": 36})

serve.create_endpoint("router_failure", "/router_failure", methods=["GET"])

def function():
    return "hello1"

serve.create_backend(function, "router_failure:v1")
serve.link("router_failure", "router_failure:v1")

assert request_with_retries(
    "/router_failure", timeout=5).text == "hello1"

for _ in range(10):
    response = request_with_retries("/router_failure", timeout=30)
    assert response.text == "hello1"

_kill_router()
time.sleep(10)
