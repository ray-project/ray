import logging
from concurrent import futures
import grpc
from ray import cloudpickle
import ray
import ray.state
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
import time
import inspect
import json
from ray.experimental.client import stash_api_for_tests, _set_server_api
from ray.experimental.client.common import convert_from_arg
from ray.experimental.client.common import encode_exception
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

    def ClusterInfo(self, request,
                    context=None) -> ray_client_pb2.ClusterInfoResponse:
        resp = ray_client_pb2.ClusterInfoResponse()
        resp.type = request.type
        if request.type == ray_client_pb2.ClusterInfoType.CLUSTER_RESOURCES:
            resources = ray.cluster_resources()
            # Normalize resources into floats
            # (the function may return values that are ints)
            float_resources = {k: float(v) for k, v in resources.items()}
            resp.resource_table.CopyFrom(
                ray_client_pb2.ClusterInfoResponse.ResourceTable(
                    table=float_resources))
        elif request.type == \
                ray_client_pb2.ClusterInfoType.AVAILABLE_RESOURCES:
            resources = ray.available_resources()
            # Normalize resources into floats
            # (the function may return values that are ints)
            float_resources = {k: float(v) for k, v in resources.items()}
            resp.resource_table.CopyFrom(
                ray_client_pb2.ClusterInfoResponse.ResourceTable(
                    table=float_resources))
        else:
            resp.json = self._return_debug_cluster_info(request, context)
        return resp

    def _return_debug_cluster_info(self, request, context=None) -> str:
        data = None
        if request.type == ray_client_pb2.ClusterInfoType.NODES:
            data = ray.nodes()
        elif request.type == ray_client_pb2.ClusterInfoType.IS_INITIALIZED:
            data = ray.is_initialized()
        else:
            raise TypeError("Unsupported cluster info type")
        return json.dumps(data)

    def Terminate(self, request, context=None):
        if request.WhichOneof("terminate_type") == "task_object":
            try:
                object_ref = cloudpickle.loads(request.task_object.handle)
                ray.cancel(
                    object_ref,
                    force=request.task_object.force,
                    recursive=request.task_object.recursive)
            except Exception as e:
                return_exception_in_context(e, context)
        elif request.WhichOneof("terminate_type") == "actor":
            try:
                actor_ref = cloudpickle.loads(request.actor.handle)
                ray.kill(actor_ref, no_restart=request.actor.no_restart)
            except Exception as e:
                return_exception_in_context(e, context)
        else:
            raise RuntimeError(
                "Client requested termination without providing a valid "
                "terminate_type")
        return ray_client_pb2.TerminateResponse(ok=True)

    def GetObject(self, request, context=None):
        request_ref = cloudpickle.loads(request.handle)
        if request_ref.binary() not in self.object_refs:
            return ray_client_pb2.GetResponse(valid=False)
        objectref = self.object_refs[request_ref.binary()]
        logger.info("get: %s" % objectref)
        try:
            item = ray.get(objectref, timeout=request.timeout)
        except Exception as e:
            return_exception_in_context(e, context)
        item_ser = cloudpickle.dumps(item)
        return ray_client_pb2.GetResponse(valid=True, data=item_ser)

    def PutObject(self, request, context=None) -> ray_client_pb2.PutResponse:
        obj = cloudpickle.loads(request.data)
        objectref = self._put_and_retain_obj(obj)
        pickled_ref = cloudpickle.dumps(objectref)
        return ray_client_pb2.PutResponse(
            ref=make_remote_ref(objectref.binary(), pickled_ref))

    def _put_and_retain_obj(self, obj) -> ray.ObjectRef:
        objectref = ray.put(obj)
        self.object_refs[objectref.binary()] = objectref
        logger.info("put: %s" % objectref)
        return objectref

    def WaitObject(self, request, context=None) -> ray_client_pb2.WaitResponse:
        object_refs = [cloudpickle.loads(o) for o in request.object_handles]
        num_returns = request.num_returns
        timeout = request.timeout
        object_refs_ids = []
        for object_ref in object_refs:
            if object_ref.binary() not in self.object_refs:
                return ray_client_pb2.WaitResponse(valid=False)
            object_refs_ids.append(self.object_refs[object_ref.binary()])
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
            make_remote_ref(
                id=ready_object_ref.binary(),
                handle=cloudpickle.dumps(ready_object_ref),
            ) for ready_object_ref in ready_object_refs
        ]
        remaining_object_ids = [
            make_remote_ref(
                id=remaining_object_ref.binary(),
                handle=cloudpickle.dumps(remaining_object_ref),
            ) for remaining_object_ref in remaining_object_refs
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
            pickled_ref = cloudpickle.dumps(output)
        return ray_client_pb2.ClientTaskTicket(
            return_ref=make_remote_ref(output.binary(), pickled_ref))

    def _schedule_actor(self,
                        task: ray_client_pb2.ClientTask,
                        context=None,
                        prepared_args=None) -> ray_client_pb2.ClientTaskTicket:
        with stash_api_for_tests(self._test_mode):
            payload_ref = cloudpickle.loads(task.payload_id)
            if payload_ref.binary() not in self.registered_actor_classes:
                actor_class_ref = self.object_refs[payload_ref.binary()]
                actor_class = ray.get(actor_class_ref)
                if not inspect.isclass(actor_class):
                    raise Exception("Attempting to schedule actor that "
                                    "isn't a class.")
                reg_class = ray.remote(actor_class)
                self.registered_actor_classes[payload_ref.binary()] = reg_class
            remote_class = self.registered_actor_classes[payload_ref.binary()]
            arglist = _convert_args(task.args, prepared_args)
            actor = remote_class.remote(*arglist)
            actorhandle = cloudpickle.dumps(actor)
            self.actor_refs[actorhandle] = actor
        return ray_client_pb2.ClientTaskTicket(
            return_ref=make_remote_ref(actor._actor_id.binary(), actorhandle))

    def _schedule_function(
            self,
            task: ray_client_pb2.ClientTask,
            context=None,
            prepared_args=None) -> ray_client_pb2.ClientTaskTicket:
        payload_ref = cloudpickle.loads(task.payload_id)
        if payload_ref.binary() not in self.function_refs:
            funcref = self.object_refs[payload_ref.binary()]
            func = ray.get(funcref)
            if not inspect.isfunction(func):
                raise Exception("Attempting to schedule function that "
                                "isn't a function.")
            self.function_refs[payload_ref.binary()] = ray.remote(func)
        remote_func = self.function_refs[payload_ref.binary()]
        arglist = _convert_args(task.args, prepared_args)
        # Prepare call if we're in a test
        with stash_api_for_tests(self._test_mode):
            output = remote_func.remote(*arglist)
            if output.binary() in self.object_refs:
                raise Exception("already found it")
            self.object_refs[output.binary()] = output
            pickled_output = cloudpickle.dumps(output)
        return ray_client_pb2.ClientTaskTicket(
            return_ref=make_remote_ref(output.binary(), pickled_output))


def _convert_args(arg_list, prepared_args=None):
    if prepared_args is not None:
        return prepared_args
    out = []
    for arg in arg_list:
        t = convert_from_arg(arg)
        if isinstance(t, ClientObjectRef):
            out.append(t._unpack_ref())
        else:
            out.append(t)
    return out


def make_remote_ref(id: bytes, handle: bytes) -> ray_client_pb2.RemoteRef:
    return ray_client_pb2.RemoteRef(
        id=id,
        handle=handle,
    )


def return_exception_in_context(err, context):
    if context is not None:
        context.set_details(encode_exception(err))
        context.set_code(grpc.StatusCode.INTERNAL)


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
