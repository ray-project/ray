import logging
from concurrent import futures
import grpc
from ray import cloudpickle
import ray
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
import time
from ray.experimental.client.core_ray_api import set_client_api_as_ray
from ray.experimental.client.common import convert_from_arg
from ray.experimental.client.common import ObjectID


class RayletServicer(ray_client_pb2_grpc.RayletDriverServicer):
    def __init__(self):
        self.object_refs = {}
        self.function_refs = {}

    def GetObject(self, request, context=None):
        if request.id not in self.object_refs:
            return ray_client_pb2.GetResponse(valid=False)
        objectref = self.object_refs[request.id]
        print("get: %s" % objectref)
        item = ray.get(objectref)
        item_ser = cloudpickle.dumps(item)
        return ray_client_pb2.GetResponse(valid=True, data=item_ser)

    def PutObject(self, request, context=None):
        obj = cloudpickle.loads(request.data)
        objectref = ray.put(obj)
        self.object_refs[objectref.binary()] = objectref
        print("put: %s" % objectref)
        return ray_client_pb2.PutResponse(id=objectref.binary())

    def Schedule(self, task, context=None):
        print("Got Schedule: ", task)
        if task.payload_id not in self.function_refs:
            funcref = self.object_refs[task.payload_id]
            print("funcref: ", funcref)
            func = ray.get(funcref)
            self.function_refs[task.payload_id] = ray.remote(func)
        remote_func = self.function_refs[task.payload_id]
        print("Wrapped func:", remote_func)
        # TODO(barakmich): decode args
        arglist = _convert_args(task.args)
        output = remote_func.remote(*arglist)
        print("Call Object", output)
        self.object_refs[output.binary()] = output
        return ray_client_pb2.ClientTaskTicket(return_id=output.binary())


def _convert_args(arg_list):
    out = []
    for arg in arg_list:
        t = convert_from_arg(arg)
        if isinstance(t, ObjectID):
            out.append(ray.ObjectRef(t.id))
        else:
            out.append(t)
    return out


def serve(connection_str):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    task_servicer = RayletServicer()
    ray_client_pb2_grpc.add_RayletDriverServicer_to_server(
        task_servicer, server)
    server.add_insecure_port(connection_str)
    server.start()
    return server


if __name__ == "__main__":
    logging.basicConfig()
    # TODO(barakmich): Perhaps wrap ray init
    ray.init()
    set_client_api_as_ray()
    server = serve("0.0.0.0:50051")
    try:
        while True:
            time.sleep(1000)
    except KeyboardInterrupt:
        server.stop(0)
