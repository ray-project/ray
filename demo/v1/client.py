import ray
from rpc_pb2 import Request, Response

ray.init(address="auto", namespace="demo")
actor = ray.get_actor("ServerActor")
for age in [10, 22]:
    print("Asking about age", age)
    ref = actor.handle_request.remote(Request(age=age))
    out_proto = ray.get(ref)
    print("Result is", out_proto.drink_ok)
