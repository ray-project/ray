import pytest
import sys

scripts = """
import ray
import json
from fastapi import FastAPI
app = FastAPI()
from ray import serve
ray.init(address="auto", namespace="gcs")

@serve.deployment(name="Counter", route_prefix="/api")
@serve.ingress(app)
class Counter:
    def __init__(self):
        self.count = 0

    @app.get("/")
    def get(self):
        return {{"count": self.count}}

    @app.get("/incr")
    def incr(self):
        self.count += 1
        return {{"count": self.count}}

    @app.get("/decr")
    def decr(self):
        self.count -= 1
        return {{"count": self.count}}

    @app.get("/pid")
    def pid(self):
        import os
        return {{"pid": os.getpid()}}
serve.start(detached=True, http_options={{"location":"EveryNode"}})
Counter.options(num_replicas={num_replicas}).deploy()
"""

check_script = f"""
import requests
import json
b = json.loads(requests.get("http://127.0.0.1:8000/api/").text)["count"]
for i in range(5):
    response = requests.get("http://127.0.0.1:8000/api/incr")
    assert json.loads(response.text) == {{"count": i + b + 1}}

pids = {{
    json.loads(requests.get("http://127.0.0.1:8000/api/pid").text)["pid"]
    for _ in range(5)
}}

assert len(pids) == 1
"""

def test_ray_server(docker_cluster):
    header, worker = docker_cluster
    output = worker.exec_run(cmd=f"python -c '{scripts.format(num_replicas=1)}'")
    assert output.exit_code == 0
    assert b"Adding 1 replicas to deployment 'Counter'." in output.output
    # somehow this is not working and the port is not exposed to the host.
    # worker_cli = worker.client()
    # print(worker_cli.request("GET", "/api/incr"))
    print(">>>> SCRIPT <<<<")
    print(check_script)

    output =  worker.exec_run(cmd=f"python -c '{check_script}'")

    assert output.exit_code == 0
    # Kill the head node

    header.kill()
    import pdb
    pdb.set_trace()
    # Make sure serve is still working
    output =  worker.exec_run(cmd=f"python -c '{check_script}'")
    assert output.exit_code == 0



if __name__ == "__main__":
    sys.exit(pytest.main(["-vs", __file__]))
