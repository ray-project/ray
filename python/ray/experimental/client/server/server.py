import logging
from concurrent import futures
import grpc
import base64
from collections import defaultdict

from typing import Dict
from typing import Set

from ray import cloudpickle
import ray
import ray.state
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
import time
import inspect
import json
from ray.experimental.client import stash_api_for_tests, _set_server_api
from ray.experimental.client.server.server_pickler import convert_from_arg
from ray.experimental.client.server.server_pickler import dumps_from_server
from ray.experimental.client.server.server_pickler import loads_from_client
from ray.experimental.client.server.core_ray_api import RayServerAPI
from ray.experimental.client.server.dataservicer import DataServicer
from ray.experimental.client.server.server_stubs import current_func

logger = logging.getLogger(__name__)


class RayletServicer(ray_client_pb2_grpc.RayletDriverServicer):
    def __init__(self, test_mode=False):
        self.object_refs: Dict[str, Dict[bytes, ray.ObjectRef]] = defaultdict(
            dict)
        self.function_refs = {}
        self.actor_refs: Dict[bytes, ray.ActorHandle] = {}
        self.actor_owners: Dict[str, Set[bytes]] = defaultdict(set)
        self.registered_actor_classes = {}
        self._test_mode = test_mode
        self._current_function_stub = None

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

    def release(self, client_id: str, id: bytes) -> bool:
        if client_id in self.object_refs:
            if id in self.object_refs[client_id]:
                logger.debug(f"Releasing object {id.hex()} for {client_id}")
                del self.object_refs[client_id][id]
                return True

        if client_id in self.actor_owners:
            if id in self.actor_owners[client_id]:
                logger.debug(f"Releasing actor {id.hex()} for {client_id}")
                del self.actor_refs[id]
                self.actor_owners[client_id].remove(id)
                return True

        return False

    def release_all(self, client_id):
        self._release_objects(client_id)
        self._release_actors(client_id)

    def _release_objects(self, client_id):
        if client_id not in self.object_refs:
            logger.debug(f"Releasing client with no references: {client_id}")
            return
        count = len(self.object_refs[client_id])
        del self.object_refs[client_id]
        logger.debug(f"Released all {count} objects for client {client_id}")

    def _release_actors(self, client_id):
        if client_id not in self.actor_owners:
            logger.debug(f"Releasing client with no actors: {client_id}")
        count = 0
        for id_bytes in self.actor_owners[client_id]:
            count += 1
            del self.actor_refs[id_bytes]
        del self.actor_owners[client_id]
        logger.debug(f"Released all {count} actors for client: {client_id}")

    def Terminate(self, req, context=None):
        if req.WhichOneof("terminate_type") == "task_object":
            try:
                object_ref = \
                        self.object_refs[req.client_id][req.task_object.id]
                ray.cancel(
                    object_ref,
                    force=req.task_object.force,
                    recursive=req.task_object.recursive)
            except Exception as e:
                return_exception_in_context(e, context)
        elif req.WhichOneof("terminate_type") == "actor":
            try:
                actor_ref = self.actor_refs[req.actor.id]
                ray.kill(actor_ref, no_restart=req.actor.no_restart)
            except Exception as e:
                return_exception_in_context(e, context)
        else:
            raise RuntimeError(
                "Client requested termination without providing a valid "
                "terminate_type")
        return ray_client_pb2.TerminateResponse(ok=True)

    def GetObject(self, request, context=None):
        return self._get_object(request, "", context)

    def _get_object(self, request, client_id: str, context=None):
        if request.id not in self.object_refs[client_id]:
            return ray_client_pb2.GetResponse(valid=False)
        objectref = self.object_refs[client_id][request.id]
        logger.debug("get: %s" % objectref)
        try:
            item = ray.get(objectref, timeout=request.timeout)
        except Exception as e:
            return ray_client_pb2.GetResponse(
                valid=False, error=cloudpickle.dumps(e))
        item_ser = dumps_from_server(item, client_id, self)
        return ray_client_pb2.GetResponse(valid=True, data=item_ser)

    def PutObject(self, request: ray_client_pb2.PutRequest,
                  context=None) -> ray_client_pb2.PutResponse:
        """gRPC entrypoint for unary PutObject
        """
        return self._put_object(request, "", context)

    def _put_object(self,
                    request: ray_client_pb2.PutRequest,
                    client_id: str,
                    context=None):
        """Put an object in the cluster with ray.put() via gRPC.

        Args:
            request: PutRequest with pickled data.
            client_id: The client who owns this data, for tracking when to
              delete this reference.
            context: gRPC context.
        """
        obj = loads_from_client(request.data, self)
        objectref = ray.put(obj)
        self.object_refs[client_id][objectref.binary()] = objectref
        logger.debug("put: %s" % objectref)
        return ray_client_pb2.PutResponse(id=objectref.binary())

    def WaitObject(self, request, context=None) -> ray_client_pb2.WaitResponse:
        object_refs = []
        for id in request.object_ids:
            if id not in self.object_refs[request.client_id]:
                raise Exception(
                    "Asking for a ref not associated with this client: %s" %
                    str(id))
            object_refs.append(self.object_refs[request.client_id][id])
        num_returns = request.num_returns
        timeout = request.timeout
        try:
            ready_object_refs, remaining_object_refs = ray.wait(
                object_refs,
                num_returns=num_returns,
                timeout=timeout if timeout != -1 else None)
        except Exception:
            # TODO(ameer): improve exception messages.
            return ray_client_pb2.WaitResponse(valid=False)
        logger.debug("wait: %s %s" % (str(ready_object_refs),
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
        with stash_api_for_tests(self._test_mode):
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
        arglist = self._convert_args(task.args, prepared_args)
        output = getattr(actor_handle, task.name).remote(*arglist)
        self.object_refs[task.client_id][output.binary()] = output
        return ray_client_pb2.ClientTaskTicket(return_id=output.binary())

    def _schedule_actor(self,
                        task: ray_client_pb2.ClientTask,
                        context=None,
                        prepared_args=None) -> ray_client_pb2.ClientTaskTicket:
        if task.payload_id not in self.registered_actor_classes:
            actor_class_ref = \
                    self.object_refs[task.client_id][task.payload_id]
            actor_class = ray.get(actor_class_ref)
            if not inspect.isclass(actor_class):
                raise Exception("Attempting to schedule actor that "
                                "isn't a class.")
            reg_class = ray.remote(actor_class)
            self.registered_actor_classes[task.payload_id] = reg_class
        remote_class = self.registered_actor_classes[task.payload_id]
        arglist = self._convert_args(task.args, prepared_args)
        actor = remote_class.remote(*arglist)
        self.actor_refs[actor._actor_id.binary()] = actor
        self.actor_owners[task.client_id].add(actor._actor_id.binary())
        return ray_client_pb2.ClientTaskTicket(
            return_id=actor._actor_id.binary())

    def _schedule_function(
            self,
            task: ray_client_pb2.ClientTask,
            context=None,
            prepared_args=None) -> ray_client_pb2.ClientTaskTicket:
        remote_func = self.lookup_or_register_func(task.payload_id,
                                                   task.client_id)
        arglist = self._convert_args(task.args, prepared_args)
        # Prepare call if we're in a test
        with current_func(remote_func):
            output = remote_func.remote(*arglist)
        if output.binary() in self.object_refs[task.client_id]:
            raise Exception("already found it")
        self.object_refs[task.client_id][output.binary()] = output
        return ray_client_pb2.ClientTaskTicket(return_id=output.binary())

    def _convert_args(self, arg_list, prepared_args=None):
        if prepared_args is not None:
            return prepared_args
        out = []
        for arg in arg_list:
            t = convert_from_arg(arg, self)
            out.append(t)
        return out

    def lookup_or_register_func(self, id: bytes, client_id: str
                                ) -> ray.remote_function.RemoteFunction:
        if id not in self.function_refs:
            funcref = self.object_refs[client_id][id]
            func = ray.get(funcref)
            if not inspect.isfunction(func):
                raise Exception("Attempting to register function that "
                                "isn't a function.")
            self.function_refs[id] = ray.remote(func)
        return self.function_refs[id]


def return_exception_in_context(err, context):
    if context is not None:
        context.set_details(encode_exception(err))
        context.set_code(grpc.StatusCode.INTERNAL)


def encode_exception(exception) -> str:
    data = cloudpickle.dumps(exception)
    return base64.standard_b64encode(data).decode()


def serve(connection_str, test_mode=False):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    task_servicer = RayletServicer(test_mode=test_mode)
    data_servicer = DataServicer(task_servicer)
    _set_server_api(RayServerAPI(task_servicer))
    ray_client_pb2_grpc.add_RayletDriverServicer_to_server(
        task_servicer, server)
    ray_client_pb2_grpc.add_RayletDataStreamerServicer_to_server(
        data_servicer, server)
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
