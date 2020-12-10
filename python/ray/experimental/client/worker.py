"""This file includes the Worker class which sits on the client side.
It implements the Ray API functions that are forwarded through grpc calls
to the server.
"""
import inspect
import logging
from typing import List
from typing import Tuple

import ray.cloudpickle as cloudpickle
from ray.util.inspect import is_cython
import grpc

import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.experimental.client.common import convert_to_arg
from ray.experimental.client.common import ClientObjectRef
from ray.experimental.client.common import ClientActorClass
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
        if stub is None:
            if secure:
                credentials = grpc.ssl_channel_credentials()
                self.channel = grpc.secure_channel(conn_str, credentials)
            else:
                self.channel = grpc.insecure_channel(conn_str)
            self.server = ray_client_pb2_grpc.RayletDriverStub(self.channel)
        else:
            self.server = stub

    def get(self, ids):
        to_get = []
        single = False
        if isinstance(ids, list):
            to_get = [x.id for x in ids]
        elif isinstance(ids, ClientObjectRef):
            to_get = [ids.id]
            single = True
        else:
            raise Exception("Can't get something that's not a "
                            "list of IDs or just an ID: %s" % type(ids))
        out = [self._get(x) for x in to_get]
        if single:
            out = out[0]
        return out

    def _get(self, id: bytes):
        req = ray_client_pb2.GetRequest(id=id)
        data = self.server.GetObject(req, metadata=self.metadata)
        if not data.valid:
            raise Exception(
                "Client GetObject returned invalid data: id invalid?")
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
        return ClientObjectRef(resp.id)

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
            "object_refs": [
                cloudpickle.dumps(object_ref) for object_ref in object_refs
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
            ClientObjectRef(id) for id in resp.ready_object_ids
        ]
        client_remaining_object_ids = [
            ClientObjectRef(id) for id in resp.remaining_object_ids
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
        return ClientObjectRef(ticket.return_id)

    def close(self):
        self.channel.close()
