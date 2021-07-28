import logging
from concurrent import futures
import grpc
import base64
from collections import defaultdict
import os
import queue
import pickle

import threading
from typing import Any
from typing import Dict
from typing import Set
from typing import Optional
from typing import Callable
from ray import cloudpickle
from ray.job_config import JobConfig
import ray
import ray.state
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
import time
import inspect
import json
from ray.util.client.common import (ClientServerHandle, GRPC_OPTIONS,
                                    CLIENT_SERVER_MAX_THREADS)
from ray.util.client.server.proxier import serve_proxier
from ray.util.client.server.server_pickler import convert_from_arg
from ray.util.client.server.server_pickler import dumps_from_server
from ray.util.client.server.server_pickler import loads_from_client
from ray.util.client.server.dataservicer import DataServicer
from ray.util.client.server.logservicer import LogstreamServicer
from ray.util.client.server.server_stubs import current_server
from ray.ray_constants import env_integer
from ray.util.placement_group import PlacementGroup
from ray._private.client_mode_hook import disable_client_hook

logger = logging.getLogger(__name__)

TIMEOUT_FOR_SPECIFIC_SERVER_S = env_integer("TIMEOUT_FOR_SPECIFIC_SERVER_S",
                                            30)


class RayletServicer(ray_client_pb2_grpc.RayletDriverServicer):
    def __init__(self, ray_connect_handler: Callable):
        """Construct a raylet service

        Args:
           ray_connect_handler (Callable): Function to connect to ray cluster
        """
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
        self.ray_connect_handler = ray_connect_handler

    def Init(self, request, context=None) -> ray_client_pb2.InitResponse:
        if request.job_config:
            job_config = pickle.loads(request.job_config)
            job_config.client_job = True
        else:
            job_config = None
        current_job_config = None
        with disable_client_hook():
            if ray.is_initialized():
                worker = ray.worker.global_worker
                current_job_config = worker.core_worker.get_job_config()
            else:
                self.ray_connect_handler(job_config)
        if job_config is None:
            return ray_client_pb2.InitResponse(ok=True)
        job_config = job_config.get_proto_job_config()
        # If the server has been initialized, we need to compare whether the
        # runtime env is compatible.
        if current_job_config and \
           set(job_config.runtime_env.uris) != set(
                current_job_config.runtime_env.uris) and \
                len(job_config.runtime_env.uris) > 0:
            return ray_client_pb2.InitResponse(
                ok=False,
                msg="Runtime environment doesn't match "
                f"request one {job_config.runtime_env.uris} "
                f"current one {current_job_config.runtime_env.uris}")
        return ray_client_pb2.InitResponse(ok=True)

    def PrepRuntimeEnv(self, request,
                       context=None) -> ray_client_pb2.PrepRuntimeEnvResponse:
        job_config = ray.worker.global_worker.core_worker.get_job_config()
        try:
            self._prepare_runtime_env(job_config.runtime_env)
        except Exception as e:
            raise grpc.RpcError(f"Prepare runtime env failed with {e}")
        return ray_client_pb2.PrepRuntimeEnvResponse()

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

    def KVExists(self, request,
                 context=None) -> ray_client_pb2.KVExistsResponse:
        with disable_client_hook():
            exists = ray.experimental.internal_kv._internal_kv_exists(
                request.key)
        return ray_client_pb2.KVExistsResponse(exists=exists)

    def ListNamedActors(self, request, context=None
                        ) -> ray_client_pb2.ClientListNamedActorsResponse:
        with disable_client_hook():
            actors = ray.util.list_named_actors(
                all_namespaces=request.all_namespaces)

        return ray_client_pb2.ClientListNamedActorsResponse(
            actors_json=json.dumps(actors))

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
                ctx.namespace = rtc.namespace
                ctx.capture_client_tasks = \
                    rtc.should_capture_child_tasks_in_placement_group
                ctx.runtime_env = json.dumps(rtc.runtime_env)
            resp.runtime_context.CopyFrom(ctx)
        else:
            with disable_client_hook():
                resp.json = self._return_debug_cluster_info(request, context)
        return resp

    def _return_debug_cluster_info(self, request, context=None) -> str:
        """Handle ClusterInfo requests that only return a json blob."""
        data = None
        if request.type == ray_client_pb2.ClusterInfoType.NODES:
            data = ray.nodes()
        elif request.type == ray_client_pb2.ClusterInfoType.IS_INITIALIZED:
            data = ray.is_initialized()
        elif request.type == ray_client_pb2.ClusterInfoType.TIMELINE:
            data = ray.timeline()
        elif request.type == ray_client_pb2.ClusterInfoType.PING:
            data = {}
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

    def _async_get_object(
            self,
            request,
            client_id: str,
            req_id: int,
            result_queue: queue.Queue,
            context=None) -> Optional[ray_client_pb2.GetResponse]:
        """Attempts to schedule a callback to push the GetResponse to the
        main loop when the desired object is ready. If there is some failure
        in scheduling, a GetResponse will be immediately returned.
        """
        if request.id not in self.object_refs[client_id]:
            return ray_client_pb2.GetResponse(valid=False)
        try:
            object_ref = self.object_refs[client_id][request.id]
            logger.debug("async get: %s" % object_ref)
            with disable_client_hook():

                def send_get_response(result: Any) -> None:
                    """Pushes a GetResponse to the main DataPath loop to send
                    to the client. This is called when the object is ready
                    on the server side."""
                    try:
                        serialized = dumps_from_server(result, client_id, self)
                        get_resp = ray_client_pb2.GetResponse(
                            valid=True, data=serialized)
                    except Exception as e:
                        get_resp = ray_client_pb2.GetResponse(
                            valid=False, error=cloudpickle.dumps(e))

                    resp = ray_client_pb2.DataResponse(
                        get=get_resp, req_id=req_id)
                    resp.req_id = req_id

                    result_queue.put(resp)

                object_ref._on_completed(send_get_response)
                return None
        except Exception as e:
            return ray_client_pb2.GetResponse(
                valid=False, error=cloudpickle.dumps(e))

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
        try:
            obj = loads_from_client(request.data, self)
            with disable_client_hook():
                objectref = ray.put(obj)
        except Exception as e:
            logger.exception("Put failed:")
            return ray_client_pb2.PutResponse(
                id=b"", valid=False, error=cloudpickle.dumps(e))

        self.object_refs[client_id][objectref.binary()] = objectref
        if len(request.client_ref_id) > 0:
            self.client_side_ref_map[client_id][
                request.client_ref_id] = objectref.binary()
        logger.debug("put: %s" % objectref)
        return ray_client_pb2.PutResponse(id=objectref.binary(), valid=True)

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

    def _prepare_runtime_env(self, job_runtime_env):
        """Download runtime environment to local node"""
        uris = job_runtime_env.uris
        from ray._private import runtime_env
        with disable_client_hook():
            working_dir = runtime_env.ensure_runtime_env_setup(uris)
            if working_dir:
                os.chdir(working_dir)

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

    if isinstance(opts.get("placement_group", None), dict):
        # Placement groups in Ray client options are serialized as dicts.
        # Convert the dict to a PlacementGroup.
        opts["placement_group"] = PlacementGroup.from_dict(
            opts["placement_group"])

    return opts


def serve(connection_str, ray_connect_handler=None):
    def default_connect_handler(job_config: JobConfig = None):
        with disable_client_hook():
            if not ray.is_initialized():
                return ray.init(job_config=job_config)

    ray_connect_handler = ray_connect_handler or default_connect_handler
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=CLIENT_SERVER_MAX_THREADS),
        options=GRPC_OPTIONS)
    task_servicer = RayletServicer(ray_connect_handler)
    data_servicer = DataServicer(task_servicer)
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

    def ray_connect_handler(job_config=None):
        # Ray client will disconnect from ray when
        # num_clients == 0.
        if ray.is_initialized():
            return info
        else:
            return ray.init(job_config=job_config, *args, **kwargs)

    server_handle = serve(
        connection_str, ray_connect_handler=ray_connect_handler)
    return (server_handle, info)


def shutdown_with_server(server, _exiting_interpreter=False):
    server.stop(1)
    with disable_client_hook():
        ray.shutdown(_exiting_interpreter)


def create_ray_handler(redis_address, redis_password):
    def ray_connect_handler(job_config: JobConfig = None):
        if redis_address:
            if redis_password:
                ray.init(
                    address=redis_address,
                    _redis_password=redis_password,
                    job_config=job_config)
            else:
                ray.init(address=redis_address, job_config=job_config)
        else:
            ray.init(job_config=job_config)

    return ray_connect_handler


def try_create_redis_client(redis_address: Optional[str],
                            redis_password: Optional[str]) -> Optional[Any]:
    """
    Try to create a redis client based on the the command line args or by
    autodetecting a running Ray cluster.
    """
    if redis_address is None:
        possible = ray._private.services.find_redis_address()
        if len(possible) != 1:
            return None
        redis_address = possible.pop()

    if redis_password is None:
        redis_password = ray.ray_constants.REDIS_DEFAULT_PASSWORD

    return ray._private.services.create_redis_client(redis_address,
                                                     redis_password)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Host IP to bind to")
    parser.add_argument(
        "-p", "--port", type=int, default=10001, help="Port to bind to")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["proxy", "legacy", "specific-server"],
        default="proxy")
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
    parser.add_argument(
        "--worker-shim-pid",
        required=False,
        type=int,
        default=0,
        help="The PID of the process for setup worker runtime env.")
    args, _ = parser.parse_known_args()
    logging.basicConfig(level="INFO")

    # This redis client is used for health checking. We can't use `internal_kv`
    # because it requires `ray.init` to be called, which only connect handlers
    # should do.
    redis_client = None

    ray_connect_handler = create_ray_handler(args.redis_address,
                                             args.redis_password)

    hostport = "%s:%d" % (args.host, args.port)
    logger.info(f"Starting Ray Client server on {hostport}")
    if args.mode == "proxy":
        server = serve_proxier(
            hostport, args.redis_address, redis_password=args.redis_password)
    else:
        server = serve(hostport, ray_connect_handler)

    try:
        idle_checks_remaining = TIMEOUT_FOR_SPECIFIC_SERVER_S
        while True:
            health_report = {
                "time": time.time(),
            }

            try:
                if not redis_client:
                    redis_client = try_create_redis_client(
                        args.redis_address, args.redis_password)
                redis_client.hset("healthcheck:ray_client_server", "value",
                                  json.dumps(health_report))
            except Exception as e:
                logger.error(f"[{args.mode}] Failed to put health check "
                             f"on {args.redis_address}")
                logger.exception(e)

            time.sleep(1)
            if args.mode == "specific-server":
                if server.data_servicer.num_clients > 0:
                    idle_checks_remaining = TIMEOUT_FOR_SPECIFIC_SERVER_S
                else:
                    idle_checks_remaining -= 1
                if idle_checks_remaining == 0:
                    raise KeyboardInterrupt()
                if (idle_checks_remaining % 5 == 0 and idle_checks_remaining !=
                        TIMEOUT_FOR_SPECIFIC_SERVER_S):
                    logger.info(
                        f"{idle_checks_remaining} idle checks before shutdown."
                    )

    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
