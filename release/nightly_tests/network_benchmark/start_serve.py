from starlette.requests import Request
from typing import Dict
import asyncio
import argparse
import time

from ray import serve
import ray
from ray.util.state import list_nodes

output_size = 50


def main():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("num_nodes", type=int, help="number of nodes")
    args = parser.parse_args()

    ray.init()

    i = 0
    while True:
        if i > 600:
            raise Exception("nodes not enough")

        nodes = list_nodes(filters=[("STATE", "!=", "DEAD")])
        if args.num_nodes <= len(nodes):
            break

        time.sleep(1)
        print(f"waiting for nodes... current: {len(nodes)}, target: {args.num_nodes}")
        i += 1

    # 1: Define a Ray Serve application.
    @serve.deployment(
        route_prefix="/", num_replicas=ray.available_resources().get("CPU")
    )
    class MyModelDeployment:
        def __init__(self, msg: str):
            # Initialize model state: could be very large neural net weights.
            self._msg = msg

        async def __call__(self, request: Request) -> Dict:
            await asyncio.sleep(0.01)
            return {"result": b"1" * output_size}

    app = MyModelDeployment.bind(msg="Hello world!")

    # 2: Deploy the application locally.
    serve.run(app)


main()
