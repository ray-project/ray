import subprocess

# __deploy_in_single_file_1_start__
from starlette.requests import Request

import ray
from ray import serve


@serve.deployment
def my_func(request: Request) -> str:
    return "hello"


serve.run(my_func.bind())
# __deploy_in_single_file_1_end__

serve.shutdown()
ray.shutdown()
subprocess.check_output(["ray", "stop", "--force"])
subprocess.check_output(["ray", "start", "--head"])

# __deploy_in_single_file_2_start__
# This will connect to the running Ray cluster.
ray.init(address="auto", namespace="serve")


@serve.deployment
def my_func(request: Request) -> str:
    return "hello"


serve.run(my_func.bind())
# __deploy_in_single_file_2_end__

serve.shutdown()
ray.shutdown()
subprocess.check_output(["ray", "stop", "--force"])
subprocess.check_output(["ray", "start", "--head"])

# __deploy_in_k8s_start__
# Connect to the running Ray cluster.
ray.init(address="auto")


@serve.deployment(route_prefix="/hello")
def hello(request):
    return "hello world"


serve.run(hello.bind())
# __deploy_in_k8s_end__

subprocess.check_output(["ray", "stop", "--force"])
