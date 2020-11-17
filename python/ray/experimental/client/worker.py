"""This file includes the Worker class which sits on the client side.
It implements the Ray API functions that are forwarded through grpc calls
to the server.
"""
from typing import List

import grpc

from ray import cloudpickle
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.experimental.client.common import convert_to_arg
from ray.experimental.client.common import ClientObjectRef
from ray.experimental.client.common import ClientRemoteFunc


class Worker:
    def __init__(self, conn_str="", stub=None):
        if stub is None:
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
        data = self.server.GetObject(req)
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
        resp = self.server.PutObject(req)
        return ClientObjectRef(resp.id)

    def wait(self,
             object_refs: List[ClientObjectRef],
             *,
             num_returns: int = 1,
             timeout: float = None
             ) -> (List[ClientObjectRef], List[ClientObjectRef]):
        assert isinstance(object_refs, list)
        for ref in object_refs:
            assert isinstance(ref, ClientObjectRef)
        data = {
            "object_refs": cloudpickle.dumps(object_refs),
            "num_returns": cloudpickle.dumps(num_returns),
            "timeout": cloudpickle.dumps(timeout)
        }
        req = ray_client_pb2.WaitRequest(**data)
        resp = self.server.WaitObject(req)
        if not resp.valid:
            # TODO(ameer): improve error/exceptions messages.
            raise Exception("Client Wait request failed. Reference invalid?")
        client_ready_object_ids = []
        client_remaining_object_ids = []
        for id in cloudpickle.loads(resp.ready_object_ids):
            client_ready_object_ids.append(ClientObjectRef(id))
        for id in cloudpickle.loads(resp.remaining_object_ids):
            client_remaining_object_ids.append(ClientObjectRef(id))

        return (client_ready_object_ids, client_remaining_object_ids)

    def remote(self, func):
        return ClientRemoteFunc(func)

    def call_remote(self, func, *args, **kwargs):
        if not isinstance(func, ClientRemoteFunc):
            raise TypeError("Client not passing a ClientRemoteFunc stub")
        func_ref = self._put(func)
        task = ray_client_pb2.ClientTask()
        task.name = func._name
        task.payload_id = func_ref.id
        for arg in args:
            pb_arg = convert_to_arg(arg)
            task.args.append(pb_arg)
        ticket = self.server.Schedule(task)
        return ClientObjectRef(ticket.return_id)

    def close(self):
        self.channel.close()
