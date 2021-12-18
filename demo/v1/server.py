import ray

from rpc_pb2 import Request, Response

ray.init(namespace="demo")


@ray.remote
class ServerActor:
    def __init__(self) -> None:
        # Hack:
        from rpc_pb2 import Request, Response

    def handle_request(self, request: "Request") -> "Response":
        if request.age < 21:
            return Response(drink_ok=False)
        return Response(drink_ok=True)


actor = ServerActor.options(name="ServerActor").remote()
print("actor serving")

import time
time.sleep(100)