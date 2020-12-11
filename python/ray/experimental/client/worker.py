"""This file includes the Worker class which sits on the client side.
It implements the Ray API functions that are forwarded through grpc calls
to the server.
"""
import inspect
import json
import logging
from typing import Any
from typing import List
from typing import Tuple
from typing import Optional

import ray.cloudpickle as cloudpickle
from ray.util.inspect import is_cython
import grpc

from ray.exceptions import TaskCancelledError
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.experimental.client.common import convert_to_arg
from ray.experimental.client.common import decode_exception
from ray.experimental.client.common import ClientObjectRef
from ray.experimental.client.common import ClientActorClass
from ray.experimental.client.common import ClientActorHandle
from ray.experimental.client.common import ClientRemoteFunc

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self,
                 conn_str: str = "",
                 secure: bool = False,
                 metadata: List[Tuple[str, str]] = None,
                 stub=None):
        """Initializes the worker side grpc client.

        Args:
            stub: custom grpc stub.
            secure: whether to use SSL secure channel or not.
            metadata: additional metadata passed in the grpc request headers.
        """
        self.metadata = metadata
        self.channel = None
        if stub is None:
            if secure:
                credentials = grpc.ssl_channel_credentials()
                self.channel = grpc.secure_channel(conn_str, credentials)
            else:
                self.channel = grpc.insecure_channel(conn_str)
            self.server = ray_client_pb2_grpc.RayletDriverStub(self.channel)
        else:
            self.server = stub

    def get(self, vals, *, timeout: Optional[float] = None) -> Any:
        to_get = []
        single = False
        if isinstance(vals, list):
            to_get = [x.handle for x in vals]
        elif isinstance(vals, ClientObjectRef):
            to_get = [vals.handle]
            single = True
        else:
            raise Exception("Can't get something that's not a "
                            "list of IDs or just an ID: %s" % type(vals))
        if timeout is None:
            timeout = 0
        out = [self._get(x, timeout) for x in to_get]
        if single:
            out = out[0]
        return out

    def _get(self, handle: bytes, timeout: float):
        req = ray_client_pb2.GetRequest(handle=handle, timeout=timeout)
        try:
            data = self.server.GetObject(req, metadata=self.metadata)
        except grpc.RpcError as e:
            raise decode_exception(e.details())
        if not data.valid:
            raise TaskCancelledError(handle)
        return cloudpickle.loads(data.data)

    def put(self, vals):
        to_put = []
        single = False
        if isinstance(vals, list):
            to_put = vals
        else:
            single = True
            to_put.append(vals)

        out = [self._put(x) for x in to_put]
        if single:
            out = out[0]
        return out

    def _put(self, val):
        data = cloudpickle.dumps(val)
        req = ray_client_pb2.PutRequest(data=data)
        resp = self.server.PutObject(req, metadata=self.metadata)
        return ClientObjectRef.from_remote_ref(resp.ref)

    def wait(self,
             object_refs: List[ClientObjectRef],
             *,
             num_returns: int = 1,
             timeout: float = None
             ) -> Tuple[List[ClientObjectRef], List[ClientObjectRef]]:
        assert isinstance(object_refs, list)
        for ref in object_refs:
            assert isinstance(ref, ClientObjectRef)
        data = {
            "object_handles": [
                object_ref.handle for object_ref in object_refs
            ],
            "num_returns": num_returns,
            "timeout": timeout if timeout else -1
        }
        req = ray_client_pb2.WaitRequest(**data)
        resp = self.server.WaitObject(req, metadata=self.metadata)
        if not resp.valid:
            # TODO(ameer): improve error/exceptions messages.
            raise Exception("Client Wait request failed. Reference invalid?")
        client_ready_object_ids = [
            ClientObjectRef.from_remote_ref(ref)
            for ref in resp.ready_object_ids
        ]
        client_remaining_object_ids = [
            ClientObjectRef.from_remote_ref(ref)
            for ref in resp.remaining_object_ids
        ]

        return (client_ready_object_ids, client_remaining_object_ids)

    def remote(self, function_or_class, *args, **kwargs):
        # TODO(barakmich): Arguments to ray.remote
        # get captured here.
        if (inspect.isfunction(function_or_class)
                or is_cython(function_or_class)):
            return ClientRemoteFunc(function_or_class)
        elif inspect.isclass(function_or_class):
            return ClientActorClass(function_or_class)
        else:
            raise TypeError("The @ray.remote decorator must be applied to "
                            "either a function or to a class.")

    def call_remote(self, instance, *args, **kwargs):
        task = instance._prepare_client_task()
        for arg in args:
            pb_arg = convert_to_arg(arg)
            task.args.append(pb_arg)
        logging.debug("Scheduling %s" % task)
        ticket = self.server.Schedule(task, metadata=self.metadata)
        return ClientObjectRef.from_remote_ref(ticket.return_ref)

    def close(self):
        self.server = None
        if self.channel:
            self.channel.close()

    def terminate_actor(self, actor: ClientActorHandle,
                        no_restart: bool) -> None:
        if not isinstance(actor, ClientActorHandle):
            raise ValueError("ray.kill() only supported for actors. "
                             "Got: {}.".format(type(actor)))
        term_actor = ray_client_pb2.TerminateRequest.ActorTerminate()
        term_actor.handle = actor.actor_ref.handle
        term_actor.no_restart = no_restart
        try:
            term = ray_client_pb2.TerminateRequest(actor=term_actor)
            self.server.Terminate(term)
        except grpc.RpcError as e:
            raise decode_exception(e.details())

    def terminate_task(self, obj: ClientObjectRef, force: bool,
                       recursive: bool) -> None:
        if not isinstance(obj, ClientObjectRef):
            raise TypeError(
                "ray.cancel() only supported for non-actor object refs. "
                f"Got: {type(obj)}.")
        term_object = ray_client_pb2.TerminateRequest.TaskObjectTerminate()
        term_object.handle = obj.handle
        term_object.force = force
        term_object.recursive = recursive
        try:
            term = ray_client_pb2.TerminateRequest(task_object=term_object)
            self.server.Terminate(term)
        except grpc.RpcError as e:
            raise decode_exception(e.details())

    def get_cluster_info(self, type: ray_client_pb2.ClusterInfoType.TypeEnum):
        req = ray_client_pb2.ClusterInfoRequest()
        req.type = type
        resp = self.server.ClusterInfo(req)
        if resp.WhichOneof("response_type") == "resource_table":
            return resp.resource_table.table
        return json.loads(resp.json)

    def is_initialized(self) -> bool:
        if self.server is not None:
            return self.get_cluster_info(
                ray_client_pb2.ClusterInfoType.IS_INITIALIZED)
        return False
