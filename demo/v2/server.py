from google.protobuf import message
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
            if request.name == "":
                return Response(drink_ok=False, message="name missing")
            else:
                return Response(
                    drink_ok=False, message=f"Hi {request.name} are underage")
        return Response(drink_ok=True, message=f"{request.name} is ok")


actor = ServerActor.options(name="ServerActor").remote()
print("actor serving")

import time
time.sleep(100)