import logging
from concurrent import futures
import grpc
from ray import cloudpickle
import ray
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
import time
import inspect
import json
from ray.experimental.client import stash_api_for_tests, _set_server_api
from ray.experimental.client.common import convert_from_arg
from ray.experimental.client.common import ClientObjectRef
from ray.experimental.client.server.core_ray_api import RayServerAPI

logger = logging.getLogger(__name__)


class RayletServicer(ray_client_pb2_grpc.RayletDriverServicer):
    def __init__(self, test_mode=False):
        self.object_refs = {}
        self.function_refs = {}
        self.actor_refs = {}
        self.registered_actor_classes = {}
        self._test_mode = test_mode

    def ClusterInfo(self, request, context=None) -> ray_client_pb2.ClusterInfoResponse:
        resp = ray_client_pb2.ClusterInfoResponse()
        resp.type = request.type
        if request.type == ray_client_pb2.CURRENT_NODE_ID:
            resp.id = ray.current_node_id()
        elif request.type == ray_client_pb2.CLUSTER_RESOURCES:
            resp.resource_table = ray.cluster_resources()
        elif request.type == ray_client_pb2.AVAILABLE_RESOURCES:
            resp.resource_table = ray.available_resources()
        else:
            resp.debug_table_json = self._return_debug_cluster_info(request, context)
        return resp

    def _return_debug_cluster_info(request, context=None) -> str:
        data = None
        if request.type == ray_client_pb2.JOBS:
            data = ray.jobs()
        elif request.type == ray_client_pb2.NODES:
            data = ray.nodes()
        elif request.type == ray_client_pb2.WORKERS:
            data = ray.workers()
        elif request.type == ray_client_pb2.NODE_IDS:
            data = ray.node_ids()
        elif request.type == ray_client_pb2.ACTORS:
            if request.HasField("client_id"):
                actor = self.actor_refs[request.client_id]
                data = ray.actors(actor._actor_id.hex())
            else:
                data = ray.actors()
        elif request.type == ray_client_pb2.OBJECTS:
            if request.HasField("client_id"):
                ref = self.object_refs[request.client_id]
                data = ray.objects(ref)
            else:
                data = ray.objects()
        else:
            raise TypeError("Unsupported cluster info type")
        return json.dumps(data)

    def TerminateRequest(self, request, context=None):
        if request.WhichOneof("terminate_type") == "task_object":
            obj = self.object_refs[request.task_object.id]
            ray.cancel(obj, force=request.task_object.force, recursive=request.task_object.recursive)
            def self.object_refs[request.task_object.id]
        elif request.WhichOneof("terminate_type") == "actor":
            actor = self.actor_refs[request.actor.id]
            ray.kill(actor, no_restart=request.actor.no_restart)
            del self.actor_refs[request.actor_id]
        else:
            raise RuntimeError(
                "Client requested termination without providing a valid terminate_type")
        return ray_client_pb2.TerminateResponse(ok=True)

    def GetObject(self, request, context=None):
        if request.id not in self.object_refs:
            return ray_client_pb2.GetResponse(valid=False)
        objectref = self.object_refs[request.id]
        logger.info("get: %s" % objectref)
        item = ray.get(objectref)
        item_ser = cloudpickle.dumps(item)
        return ray_client_pb2.GetResponse(valid=True, data=item_ser)

    def PutObject(self, request, context=None) -> ray_client_pb2.PutResponse:
        obj = cloudpickle.loads(request.data)
        objectref = self._put_and_retain_obj(obj)
        return ray_client_pb2.PutResponse(id=objectref.binary())

    def _put_and_retain_obj(self, obj) -> ray.ObjectRef:
        objectref = ray.put(obj)
        self.object_refs[objectref.binary()] = objectref
        logger.info("put: %s" % objectref)
        return objectref

    def WaitObject(self, request, context=None) -> ray_client_pb2.WaitResponse:
        object_refs = [cloudpickle.loads(o) for o in request.object_refs]
        num_returns = request.num_returns
        timeout = request.timeout
        object_refs_ids = []
        for object_ref in object_refs:
            if object_ref.id not in self.object_refs:
                return ray_client_pb2.WaitResponse(valid=False)
            object_refs_ids.append(self.object_refs[object_ref.id])
        try:
            ready_object_refs, remaining_object_refs = ray.wait(
                object_refs_ids,
                num_returns=num_returns,
                timeout=timeout if timeout != -1 else None)
        except Exception:
            # TODO(ameer): improve exception messages.
            return ray_client_pb2.WaitResponse(valid=False)
        logger.info("wait: %s %s" % (str(ready_object_refs),
                                     str(remaining_object_refs)))
        ready_object_ids = [
            ready_object_ref.binary() for ready_object_ref in ready_object_refs
        ]
        remaining_object_ids = [
            remaining_object_ref.binary()
            for remaining_object_ref in remaining_object_refs
        ]
        return ray_client_pb2.WaitResponse(
            valid=True,
            ready_object_ids=ready_object_ids,
            remaining_object_ids=remaining_object_ids)

    def Schedule(self, task, context=None,
                 prepared_args=None) -> ray_client_pb2.ClientTaskTicket:
        logger.info("schedule: %s %s" %
                    (task.name,
                     ray_client_pb2.ClientTask.RemoteExecType.Name(task.type)))
        if task.type == ray_client_pb2.ClientTask.FUNCTION:
            return self._schedule_function(task, context, prepared_args)
        elif task.type == ray_client_pb2.ClientTask.ACTOR:
            return self._schedule_actor(task, context, prepared_args)
        elif task.type == ray_client_pb2.ClientTask.METHOD:
            return self._schedule_method(task, context, prepared_args)
        else:
            raise NotImplementedError(
                "Unimplemented Schedule task type: %s" %
                ray_client_pb2.ClientTask.RemoteExecType.Name(task.type))

    def _schedule_method(
            self,
            task: ray_client_pb2.ClientTask,
            context=None,
            prepared_args=None) -> ray_client_pb2.ClientTaskTicket:
        actor_handle = self.actor_refs.get(task.payload_id)
        if actor_handle is None:
            raise Exception(
                "Can't run an actor the server doesn't have a handle for")
        arglist = _convert_args(task.args, prepared_args)
        with stash_api_for_tests(self._test_mode):
            output = getattr(actor_handle, task.name).remote(*arglist)
            self.object_refs[output.binary()] = output
        return ray_client_pb2.ClientTaskTicket(return_id=output.binary())

    def _schedule_actor(self,
                        task: ray_client_pb2.ClientTask,
                        context=None,
                        prepared_args=None) -> ray_client_pb2.ClientTaskTicket:
        with stash_api_for_tests(self._test_mode):
            if task.payload_id not in self.registered_actor_classes:
                actor_class_ref = self.object_refs[task.payload_id]
                actor_class = ray.get(actor_class_ref)
                if not inspect.isclass(actor_class):
                    raise Exception("Attempting to schedule actor that "
                                    "isn't a class.")
                reg_class = ray.remote(actor_class)
                self.registered_actor_classes[task.payload_id] = reg_class
            remote_class = self.registered_actor_classes[task.payload_id]
            arglist = _convert_args(task.args, prepared_args)
            actor = remote_class.remote(*arglist)
            actorhandle = cloudpickle.dumps(actor)
            self.actor_refs[actorhandle] = actor
        return ray_client_pb2.ClientTaskTicket(return_id=actorhandle)

    def _schedule_function(
            self,
            task: ray_client_pb2.ClientTask,
            context=None,
            prepared_args=None) -> ray_client_pb2.ClientTaskTicket:
        if task.payload_id not in self.function_refs:
            funcref = self.object_refs[task.payload_id]
            func = ray.get(funcref)
            if not inspect.isfunction(func):
                raise Exception("Attempting to schedule function that "
                                "isn't a function.")
            self.function_refs[task.payload_id] = ray.remote(func)
        remote_func = self.function_refs[task.payload_id]
        arglist = _convert_args(task.args, prepared_args)
        # Prepare call if we're in a test
        with stash_api_for_tests(self._test_mode):
            output = remote_func.remote(*arglist)
            if output.binary() in self.object_refs:
                raise Exception("already found it")
            self.object_refs[output.binary()] = output
        return ray_client_pb2.ClientTaskTicket(return_id=output.binary())


def _convert_args(arg_list, prepared_args=None):
    if prepared_args is not None:
        return prepared_args
    out = []
    for arg in arg_list:
        t = convert_from_arg(arg)
        if isinstance(t, ClientObjectRef):
            out.append(ray.ObjectRef(t.id))
        else:
            out.append(t)
    return out


def serve(connection_str, test_mode=False):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    task_servicer = RayletServicer(test_mode=test_mode)
    _set_server_api(RayServerAPI(task_servicer))
    ray_client_pb2_grpc.add_RayletDriverServicer_to_server(
        task_servicer, server)
    server.add_insecure_port(connection_str)
    server.start()
    return server


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    # TODO(barakmich): Perhaps wrap ray init
    ray.init()
    server = serve("0.0.0.0:50051")
    try:
        while True:
            time.sleep(1000)
    except KeyboardInterrupt:
        server.stop(0)
