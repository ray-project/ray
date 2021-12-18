import ray
from rpc_pb2 import Request, Response

ray.init(address="auto", namespace="demo")
actor = ray.get_actor("ServerActor")

for name, age in zip(["Alice", "Bob"], [10, 22]):
    print("Asking about age", age, "name", name)
    ref = actor.handle_request.remote(Request(age=age, name=name))
    out_proto = ray.get(ref)
    print("drink_ok", out_proto.drink_ok)
    print("message", out_proto.message)
