import logging
from concurrent import futures
import grpc
import base64
from collections import defaultdict
from dataclasses import dataclass

import threading
from typing import Any
from typing import Dict
from typing import Set
from typing import Optional

from ray import cloudpickle
import ray
import ray.state
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
import time
import inspect
import json
from ray.util.client.common import (GRPC_MAX_MESSAGE_SIZE,
                                    CLIENT_SERVER_MAX_THREADS)
from ray.util.client.server.server_pickler import convert_from_arg
from ray.util.client.server.server_pickler import dumps_from_server
from ray.util.client.server.server_pickler import loads_from_client
from ray.util.client.server.dataservicer import DataServicer
from ray.util.client.server.logservicer import LogstreamServicer
from ray.util.client.server.server_stubs import current_server
from ray._private.client_mode_hook import disable_client_hook

logger = logging.getLogger(__name__)


class RayletServicer(ray_client_pb2_grpc.RayletDriverServicer):
    def __init__(self):
        # Stores client_id -> (ref_id -> ObjectRef)
        self.object_refs: Dict[str, Dict[bytes, ray.ObjectRef]] = defaultdict(
            dict)
        # Stores client_id -> (client_ref_id -> ref_id (in self.object_refs))
        self.client_side_ref_map: Dict[str, Dict[bytes, bytes]] = defaultdict(
            dict)
        self.function_refs = {}
        self.actor_refs: Dict[bytes, ray.ActorHandle] = {}
        self.actor_owners: Dict[str, Set[bytes]] = defaultdict(set)
        self.registered_actor_classes = {}
        self.named_actors = set()
        self.state_lock = threading.Lock()

    def KVPut(self, request, context=None) -> ray_client_pb2.KVPutResponse:
        with disable_client_hook():
            already_exists = ray.experimental.internal_kv._internal_kv_put(
                request.key, request.value, overwrite=request.overwrite)
        return ray_client_pb2.KVPutResponse(already_exists=already_exists)

    def KVGet(self, request, context=None) -> ray_client_pb2.KVGetResponse:
        with disable_client_hook():
            value = ray.experimental.internal_kv._internal_kv_get(request.key)
        return ray_client_pb2.KVGetResponse(value=value)

    def KVDel(self, request, context=None) -> ray_client_pb2.KVDelResponse:
        with disable_client_hook():
            ray.experimental.internal_kv._internal_kv_del(request.key)
        return ray_client_pb2.KVDelResponse()

    def KVList(self, request, context=None) -> ray_client_pb2.KVListResponse:
        with disable_client_hook():
            keys = ray.experimental.internal_kv._internal_kv_list(
                request.prefix)
        return ray_client_pb2.KVListResponse(keys=keys)

    def ClusterInfo(self, request,
                    context=None) -> ray_client_pb2.ClusterInfoResponse:
        resp = ray_client_pb2.ClusterInfoResponse()
        resp.type = request.type
        if request.type == ray_client_pb2.ClusterInfoType.CLUSTER_RESOURCES:
            with disable_client_hook():
                resources = ray.cluster_resources()
            # Normalize resources into floats
            # (the function may return values that are ints)
            float_resources = {k: float(v) for k, v in resources.items()}
            resp.resource_table.CopyFrom(
                ray_client_pb2.ClusterInfoResponse.ResourceTable(
                    table=float_resources))
        elif request.type == \
                ray_client_pb2.ClusterInfoType.AVAILABLE_RESOURCES:
            with disable_client_hook():
                resources = ray.available_resources()
            # Normalize resources into floats
            # (the function may return values that are ints)
            float_resources = {k: float(v) for k, v in resources.items()}
            resp.resource_table.CopyFrom(
                ray_client_pb2.ClusterInfoResponse.ResourceTable(
                    table=float_resources))
        elif request.type == ray_client_pb2.ClusterInfoType.RUNTIME_CONTEXT:
            ctx = ray_client_pb2.ClusterInfoResponse.RuntimeContext()
            with disable_client_hook():
                rtc = ray.get_runtime_context()
                ctx.job_id = rtc.job_id.binary()
                ctx.node_id = rtc.node_id.binary()
                ctx.capture_client_tasks = \
                    rtc.should_capture_child_tasks_in_placement_group
            resp.runtime_context.CopyFrom(ctx)
        else:
            with disable_client_hook():
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
        with self.state_lock:
            if client_id in self.object_refs:
                if id in self.object_refs[client_id]:
                    logger.debug(
                        f"Releasing object {id.hex()} for {client_id}")
                    del self.object_refs[client_id][id]
                    return True

            if client_id in self.actor_owners:
                if id in self.actor_owners[client_id]:
                    logger.debug(f"Releasing actor {id.hex()} for {client_id}")
                    self.actor_owners[client_id].remove(id)
                    if self._can_remove_actor_ref(id):
                        logger.debug(f"Deleting reference to actor {id.hex()}")
                        del self.actor_refs[id]
                    return True

            return False

    def release_all(self, client_id):
        with self.state_lock:
            self._release_objects(client_id)
            self._release_actors(client_id)

    def _can_remove_actor_ref(self, actor_id_bytes):
        no_owner = not any(actor_id_bytes in actor_list
                           for actor_list in self.actor_owners.values())
        return no_owner and actor_id_bytes not in self.named_actors

    def _release_objects(self, client_id):
        if client_id not in self.object_refs:
            logger.debug(f"Releasing client with no references: {client_id}")
            return
        count = len(self.object_refs[client_id])
        del self.object_refs[client_id]
        if client_id in self.client_side_ref_map:
            del self.client_side_ref_map[client_id]
        logger.debug(f"Released all {count} objects for client {client_id}")

    def _release_actors(self, client_id):
        if client_id not in self.actor_owners:
            logger.debug(f"Releasing client with no actors: {client_id}")
            return

        count = 0
        actors_to_remove = self.actor_owners.pop(client_id)
        for id_bytes in actors_to_remove:
            count += 1
            if self._can_remove_actor_ref(id_bytes):
                logger.debug(f"Deleting reference to actor {id_bytes.hex()}")
                del self.actor_refs[id_bytes]

        logger.debug(f"Released all {count} actors for client: {client_id}")

    def Terminate(self, req, context=None):
        if req.WhichOneof("terminate_type") == "task_object":
            try:
                object_ref = \
                        self.object_refs[req.client_id][req.task_object.id]
                with disable_client_hook():
                    ray.cancel(
                        object_ref,
                        force=req.task_object.force,
                        recursive=req.task_object.recursive)
            except Exception as e:
                return_exception_in_context(e, context)
        elif req.WhichOneof("terminate_type") == "actor":
            try:
                actor_ref = self.actor_refs[req.actor.id]
                with disable_client_hook():
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
            with disable_client_hook():
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
        with disable_client_hook():
            objectref = ray.put(obj)
        self.object_refs[client_id][objectref.binary()] = objectref
        if len(request.client_ref_id) > 0:
            self.client_side_ref_map[client_id][
                request.client_ref_id] = objectref.binary()
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
            with disable_client_hook():
                ready_object_refs, remaining_object_refs = ray.wait(
                    object_refs,
                    num_returns=num_returns,
                    timeout=timeout if timeout != -1 else None,
                )
        except Exception as e:
            # TODO(ameer): improve exception messages.
            logger.error(f"Exception {e}")
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

    def Schedule(self, task, context=None) -> ray_client_pb2.ClientTaskTicket:
        logger.debug(
            "schedule: %s %s" % (task.name,
                                 ray_client_pb2.ClientTask.RemoteExecType.Name(
                                     task.type)))
        try:
            with disable_client_hook():
                if task.type == ray_client_pb2.ClientTask.FUNCTION:
                    result = self._schedule_function(task, context)
                elif task.type == ray_client_pb2.ClientTask.ACTOR:
                    result = self._schedule_actor(task, context)
                elif task.type == ray_client_pb2.ClientTask.METHOD:
                    result = self._schedule_method(task, context)
                elif task.type == ray_client_pb2.ClientTask.NAMED_ACTOR:
                    result = self._schedule_named_actor(task, context)
                else:
                    raise NotImplementedError(
                        "Unimplemented Schedule task type: %s" %
                        ray_client_pb2.ClientTask.RemoteExecType.Name(
                            task.type))
                result.valid = True
                return result
        except Exception as e:
            logger.debug(f"Caught schedule exception, returning: {e}")
            return ray_client_pb2.ClientTaskTicket(
                valid=False, error=cloudpickle.dumps(e))

    def _schedule_method(self, task: ray_client_pb2.ClientTask,
                         context=None) -> ray_client_pb2.ClientTaskTicket:
        actor_handle = self.actor_refs.get(task.payload_id)
        if actor_handle is None:
            raise Exception(
                "Can't run an actor the server doesn't have a handle for")
        arglist, kwargs = self._convert_args(task.args, task.kwargs)
        method = getattr(actor_handle, task.name)
        opts = decode_options(task.options)
        if opts is not None:
            method = method.options(**opts)
        output = method.remote(*arglist, **kwargs)
        ids = self.unify_and_track_outputs(output, task.client_id)
        return ray_client_pb2.ClientTaskTicket(return_ids=ids)

    def _schedule_actor(self, task: ray_client_pb2.ClientTask,
                        context=None) -> ray_client_pb2.ClientTaskTicket:
        remote_class = self.lookup_or_register_actor(
            task.payload_id, task.client_id,
            decode_options(task.baseline_options))

        arglist, kwargs = self._convert_args(task.args, task.kwargs)
        opts = decode_options(task.options)
        if opts is not None:
            remote_class = remote_class.options(**opts)
        with current_server(self):
            actor = remote_class.remote(*arglist, **kwargs)
        self.actor_refs[actor._actor_id.binary()] = actor
        self.actor_owners[task.client_id].add(actor._actor_id.binary())
        return ray_client_pb2.ClientTaskTicket(
            return_ids=[actor._actor_id.binary()])

    def _schedule_function(self, task: ray_client_pb2.ClientTask,
                           context=None) -> ray_client_pb2.ClientTaskTicket:
        remote_func = self.lookup_or_register_func(
            task.payload_id, task.client_id,
            decode_options(task.baseline_options))
        arglist, kwargs = self._convert_args(task.args, task.kwargs)
        opts = decode_options(task.options)
        if opts is not None:
            remote_func = remote_func.options(**opts)
        with current_server(self):
            output = remote_func.remote(*arglist, **kwargs)
        ids = self.unify_and_track_outputs(output, task.client_id)
        return ray_client_pb2.ClientTaskTicket(return_ids=ids)

    def _schedule_named_actor(self,
                              task: ray_client_pb2.ClientTask,
                              context=None) -> ray_client_pb2.ClientTaskTicket:
        assert len(task.payload_id) == 0
        actor = ray.get_actor(task.name)
        bin_actor_id = actor._actor_id.binary()
        self.actor_refs[bin_actor_id] = actor
        self.actor_owners[task.client_id].add(bin_actor_id)
        self.named_actors.add(bin_actor_id)
        return ray_client_pb2.ClientTaskTicket(
            return_ids=[actor._actor_id.binary()])

    def _convert_args(self, arg_list, kwarg_map):
        argout = []
        for arg in arg_list:
            t = convert_from_arg(arg, self)
            argout.append(t)
        kwargout = {}
        for k in kwarg_map:
            kwargout[k] = convert_from_arg(kwarg_map[k], self)
        return argout, kwargout

    def lookup_or_register_func(
            self, id: bytes, client_id: str,
            options: Optional[Dict]) -> ray.remote_function.RemoteFunction:
        with disable_client_hook():
            if id not in self.function_refs:
                funcref = self.object_refs[client_id][id]
                func = ray.get(funcref)
                if not inspect.isfunction(func):
                    raise Exception("Attempting to register function that "
                                    "isn't a function.")
                if options is None or len(options) == 0:
                    self.function_refs[id] = ray.remote(func)
                else:
                    self.function_refs[id] = ray.remote(**options)(func)
        return self.function_refs[id]

    def lookup_or_register_actor(self, id: bytes, client_id: str,
                                 options: Optional[Dict]):
        with disable_client_hook():
            if id not in self.registered_actor_classes:
                actor_class_ref = self.object_refs[client_id][id]
                actor_class = ray.get(actor_class_ref)
                if not inspect.isclass(actor_class):
                    raise Exception("Attempting to schedule actor that "
                                    "isn't a class.")
                if options is None or len(options) == 0:
                    reg_class = ray.remote(actor_class)
                else:
                    reg_class = ray.remote(**options)(actor_class)
                self.registered_actor_classes[id] = reg_class

        return self.registered_actor_classes[id]

    def unify_and_track_outputs(self, output, client_id):
        if output is None:
            outputs = []
        elif isinstance(output, list):
            outputs = output
        else:
            outputs = [output]
        for out in outputs:
            if out.binary() in self.object_refs[client_id]:
                logger.warning(f"Already saw object_ref {out}")
            self.object_refs[client_id][out.binary()] = out
        return [out.binary() for out in outputs]


def return_exception_in_context(err, context):
    if context is not None:
        context.set_details(encode_exception(err))
        context.set_code(grpc.StatusCode.INTERNAL)


def encode_exception(exception) -> str:
    data = cloudpickle.dumps(exception)
    return base64.standard_b64encode(data).decode()


def decode_options(
        options: ray_client_pb2.TaskOptions) -> Optional[Dict[str, Any]]:
    if options.json_options == "":
        return None
    opts = json.loads(options.json_options)
    assert isinstance(opts, dict)
    return opts


@dataclass
class ClientServerHandle:
    """Holds the handles to the registered gRPC servicers and their server."""
    task_servicer: RayletServicer
    data_servicer: DataServicer
    logs_servicer: LogstreamServicer
    grpc_server: grpc.Server

    # Add a hook for all the cases that previously
    # expected simply a gRPC server
    def __getattr__(self, attr):
        return getattr(self.grpc_server, attr)


def serve(connection_str, ray_connect_handler=None):
    def default_connect_handler():
        with disable_client_hook():
            if not ray.is_initialized():
                return ray.init()

    ray_connect_handler = ray_connect_handler or default_connect_handler
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=CLIENT_SERVER_MAX_THREADS),
        options=[
            ("grpc.max_send_message_length", GRPC_MAX_MESSAGE_SIZE),
            ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_SIZE),
        ])
    task_servicer = RayletServicer()
    data_servicer = DataServicer(
        task_servicer, ray_connect_handler=ray_connect_handler)
    logs_servicer = LogstreamServicer()
    ray_client_pb2_grpc.add_RayletDriverServicer_to_server(
        task_servicer, server)
    ray_client_pb2_grpc.add_RayletDataStreamerServicer_to_server(
        data_servicer, server)
    ray_client_pb2_grpc.add_RayletLogStreamerServicer_to_server(
        logs_servicer, server)
    server.add_insecure_port(connection_str)
    current_handle = ClientServerHandle(
        task_servicer=task_servicer,
        data_servicer=data_servicer,
        logs_servicer=logs_servicer,
        grpc_server=server,
    )
    server.start()
    return current_handle


def init_and_serve(connection_str, *args, **kwargs):
    with disable_client_hook():
        # Disable client mode inside the worker's environment
        info = ray.init(*args, **kwargs)

    def ray_connect_handler():
        # Ray client will disconnect from ray when
        # num_clients == 0.
        if ray.is_initialized():
            return info
        else:
            return ray.init(*args, **kwargs)

    server_handle = serve(
        connection_str, ray_connect_handler=ray_connect_handler)
    return (server_handle, info)


def shutdown_with_server(server, _exiting_interpreter=False):
    server.stop(1)
    with disable_client_hook():
        ray.shutdown(_exiting_interpreter)


def create_ray_handler(redis_address, redis_password):
    def ray_connect_handler():
        if redis_address:
            if redis_password:
                ray.init(address=redis_address, _redis_password=redis_password)
            else:
                ray.init(address=redis_address)
        else:
            ray.init()

    return ray_connect_handler


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Host IP to bind to")
    parser.add_argument(
        "-p", "--port", type=int, default=50051, help="Port to bind to")
    parser.add_argument(
        "--redis-address",
        required=False,
        type=str,
        help="Address to use to connect to Ray")
    parser.add_argument(
        "--redis-password",
        required=False,
        type=str,
        help="Password for connecting to Redis")
    args = parser.parse_args()
    logging.basicConfig(level="INFO")

    ray_connect_handler = create_ray_handler(args.redis_address,
                                             args.redis_password)

    hostport = "%s:%d" % (args.host, args.port)
    logger.info(f"Starting Ray Client server on {hostport}")
    server = serve(hostport, ray_connect_handler)
    try:
        while True:
            time.sleep(1000)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
