"""This file includes the Worker class which sits on the client side.
It implements the Ray API functions that are forwarded through grpc calls
to the server.
"""
import base64
import json
import logging
import time
import uuid
from collections import defaultdict
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Optional

import grpc

import ray.cloudpickle as cloudpickle
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.util.client.client_pickler import convert_to_arg
from ray.util.client.client_pickler import dumps_from_client
from ray.util.client.client_pickler import loads_from_server
from ray.util.client.common import ClientActorHandle
from ray.util.client.common import ClientActorRef
from ray.util.client.common import ClientObjectRef
from ray.util.client.dataclient import DataClient
from ray.util.client.logsclient import LogstreamClient

logger = logging.getLogger(__name__)

INITIAL_TIMEOUT_SEC = 5
MAX_TIMEOUT_SEC = 30


def backoff(timeout: int) -> int:
    timeout = timeout + 5
    if timeout > MAX_TIMEOUT_SEC:
        timeout = MAX_TIMEOUT_SEC
    return timeout


class Worker:
    def __init__(self,
                 conn_str: str = "",
                 secure: bool = False,
                 metadata: List[Tuple[str, str]] = None,
                 connection_retries: int = 3):
        """Initializes the worker side grpc client.

        Args:
            conn_str: The host:port connection string for the ray server.
            secure: whether to use SSL secure channel or not.
            metadata: additional metadata passed in the grpc request headers.
            connection_retries: Number of times to attempt to reconnect to the
              ray server if it doesn't respond immediately. Setting to 0 tries
              at least once.  For infinite retries, catch the ConnectionError
              exception.
        """
        self.metadata = metadata if metadata else []
        self.channel = None
        self._client_id = make_client_id()
        if secure:
            credentials = grpc.ssl_channel_credentials()
            self.channel = grpc.secure_channel(conn_str, credentials)
        else:
            self.channel = grpc.insecure_channel(conn_str)

        # Retry the connection until the channel responds to something
        # looking like a gRPC connection, though it may be a proxy.
        conn_attempts = 0
        timeout = INITIAL_TIMEOUT_SEC
        ray_ready = False
        while conn_attempts < max(connection_retries, 1):
            conn_attempts += 1
            try:
                # Let gRPC wait for us to see if the channel becomes ready.
                # If it throws, we couldn't connect.
                grpc.channel_ready_future(self.channel).result(timeout=timeout)
                # The HTTP2 channel is ready. Wrap the channel with the
                # RayletDriverStub, allowing for unary requests.
                self.server = ray_client_pb2_grpc.RayletDriverStub(
                    self.channel)
                # Now the HTTP2 channel is ready, or proxied, but the
                # servicer may not be ready. Call is_initialized() and if
                # it throws, the servicer is not ready. On success, the
                # `ray_ready` result is checked.
                ray_ready = self.is_initialized()
                if ray_ready:
                    # Ray is ready! Break out of the retry loop
                    break
                # Ray is not ready yet, wait a timeout
                time.sleep(timeout)
            except grpc.FutureTimeoutError:
                logger.info(
                    f"Couldn't connect channel in {timeout} seconds, retrying")
                # Note that channel_ready_future constitutes its own timeout,
                # which is why we do not sleep here.
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    # UNAVAILABLE is gRPC's retryable error,
                    # so we do that here.
                    logger.info("Ray client server unavailable, "
                                f"retrying in {timeout}s...")
                    logger.debug(f"Received when checking init: {e.details()}")
                    # Ray is not ready yet, wait a timeout
                    time.sleep(timeout)
                else:
                    # Any other gRPC error gets a reraise
                    raise e
            # Fallthrough, backoff, and retry at the top of the loop
            logger.info("Waiting for Ray to become ready on the server, "
                        f"retry in {timeout}s...")
            timeout = backoff(timeout)

        # If we made it through the loop without ray_ready it means we've used
        # up our retries and should error back to the user.
        if not ray_ready:
            raise ConnectionError("ray client connection timeout")

        # Initialize the streams to finish protocol negotiation.
        self.data_client = DataClient(self.channel, self._client_id,
                                      self.metadata)
        self.reference_count: Dict[bytes, int] = defaultdict(int)

        self.log_client = LogstreamClient(self.channel, self.metadata)
        self.log_client.set_logstream_level(logging.INFO)
        self.closed = False

    def connection_info(self):
        try:
            data = self.data_client.ConnectionInfo()
        except grpc.RpcError as e:
            raise e.details()
        return {
            "num_clients": data.num_clients,
            "python_version": data.python_version,
            "ray_version": data.ray_version,
            "ray_commit": data.ray_commit,
        }

    def get(self, vals, *, timeout: Optional[float] = None) -> Any:
        to_get = []
        single = False
        if isinstance(vals, list):
            to_get = vals
        elif isinstance(vals, ClientObjectRef):
            to_get = [vals]
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

    def _get(self, ref: ClientObjectRef, timeout: float):
        req = ray_client_pb2.GetRequest(id=ref.id, timeout=timeout)
        try:
            data = self.data_client.GetObject(req)
        except grpc.RpcError as e:
            raise e.details()
        if not data.valid:
            err = cloudpickle.loads(data.error)
            logger.error(err)
            raise err
        return loads_from_server(data.data)

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
        if isinstance(val, ClientObjectRef):
            raise TypeError(
                "Calling 'put' on an ObjectRef is not allowed "
                "(similarly, returning an ObjectRef from a remote "
                "function is not allowed). If you really want to "
                "do this, you can wrap the ObjectRef in a list and "
                "call 'put' on it (or return it).")
        data = dumps_from_client(val, self._client_id)
        req = ray_client_pb2.PutRequest(data=data)
        resp = self.data_client.PutObject(req)
        return ClientObjectRef(resp.id)

    def wait(self,
             object_refs: List[ClientObjectRef],
             *,
             num_returns: int = 1,
             timeout: float = None,
             fetch_local: bool = True
             ) -> Tuple[List[ClientObjectRef], List[ClientObjectRef]]:
        if not isinstance(object_refs, list):
            raise TypeError("wait() expected a list of ClientObjectRef, "
                            f"got {type(object_refs)}")
        for ref in object_refs:
            if not isinstance(ref, ClientObjectRef):
                raise TypeError("wait() expected a list of ClientObjectRef, "
                                f"got list containing {type(ref)}")
        data = {
            "object_ids": [object_ref.id for object_ref in object_refs],
            "num_returns": num_returns,
            "timeout": timeout if timeout else -1,
            "client_id": self._client_id,
        }
        req = ray_client_pb2.WaitRequest(**data)
        resp = self.server.WaitObject(req, metadata=self.metadata)
        if not resp.valid:
            # TODO(ameer): improve error/exceptions messages.
            raise Exception("Client Wait request failed. Reference invalid?")
        client_ready_object_ids = [
            ClientObjectRef(ref) for ref in resp.ready_object_ids
        ]
        client_remaining_object_ids = [
            ClientObjectRef(ref) for ref in resp.remaining_object_ids
        ]

        return (client_ready_object_ids, client_remaining_object_ids)

    def call_remote(self, instance, *args, **kwargs) -> List[bytes]:
        task = instance._prepare_client_task()
        for arg in args:
            pb_arg = convert_to_arg(arg, self._client_id)
            task.args.append(pb_arg)
        for k, v in kwargs.items():
            task.kwargs[k].CopyFrom(convert_to_arg(v, self._client_id))
        return self._call_schedule_for_task(task)

    def _call_schedule_for_task(
            self, task: ray_client_pb2.ClientTask) -> List[bytes]:
        logger.debug("Scheduling %s" % task)
        task.client_id = self._client_id
        try:
            ticket = self.server.Schedule(task, metadata=self.metadata)
        except grpc.RpcError as e:
            raise decode_exception(e.details)
        if not ticket.valid:
            raise cloudpickle.loads(ticket.error)
        return ticket.return_ids

    def call_release(self, id: bytes) -> None:
        if self.closed:
            return
        self.reference_count[id] -= 1
        if self.reference_count[id] == 0:
            self._release_server(id)
            del self.reference_count[id]

    def _release_server(self, id: bytes) -> None:
        if self.data_client is not None:
            logger.debug(f"Releasing {id}")
            self.data_client.ReleaseObject(
                ray_client_pb2.ReleaseRequest(ids=[id]))

    def call_retain(self, id: bytes) -> None:
        logger.debug(f"Retaining {id.hex()}")
        self.reference_count[id] += 1

    def close(self):
        self.log_client.close()
        self.data_client.close()
        if self.channel:
            self.channel.close()
            self.channel = None
        self.server = None
        self.closed = True

    def get_actor(self, name: str) -> ClientActorHandle:
        task = ray_client_pb2.ClientTask()
        task.type = ray_client_pb2.ClientTask.NAMED_ACTOR
        task.name = name
        ids = self._call_schedule_for_task(task)
        assert len(ids) == 1
        return ClientActorHandle(ClientActorRef(ids[0]))

    def terminate_actor(self, actor: ClientActorHandle,
                        no_restart: bool) -> None:
        if not isinstance(actor, ClientActorHandle):
            raise ValueError("ray.kill() only supported for actors. "
                             "Got: {}.".format(type(actor)))
        term_actor = ray_client_pb2.TerminateRequest.ActorTerminate()
        term_actor.id = actor.actor_ref.id
        term_actor.no_restart = no_restart
        try:
            term = ray_client_pb2.TerminateRequest(actor=term_actor)
            term.client_id = self._client_id
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
        term_object.id = obj.id
        term_object.force = force
        term_object.recursive = recursive
        try:
            term = ray_client_pb2.TerminateRequest(task_object=term_object)
            term.client_id = self._client_id
            self.server.Terminate(term)
        except grpc.RpcError as e:
            raise decode_exception(e.details())

    def get_cluster_info(self, type: ray_client_pb2.ClusterInfoType.TypeEnum):
        req = ray_client_pb2.ClusterInfoRequest()
        req.type = type
        resp = self.server.ClusterInfo(req, metadata=self.metadata)
        if resp.WhichOneof("response_type") == "resource_table":
            # translate from a proto map to a python dict
            output_dict = {k: v for k, v in resp.resource_table.table.items()}
            return output_dict
        elif resp.WhichOneof("response_type") == "runtime_context":
            return resp.runtime_context
        return json.loads(resp.json)

    def internal_kv_get(self, key: bytes) -> bytes:
        req = ray_client_pb2.KVGetRequest(key=key)
        resp = self.server.KVGet(req, metadata=self.metadata)
        return resp.value

    def internal_kv_put(self, key: bytes, value: bytes,
                        overwrite: bool) -> bool:
        req = ray_client_pb2.KVPutRequest(
            key=key, value=value, overwrite=overwrite)
        resp = self.server.KVPut(req, metadata=self.metadata)
        return resp.already_exists

    def internal_kv_del(self, key: bytes) -> None:
        req = ray_client_pb2.KVDelRequest(key=key)
        self.server.KVDel(req, metadata=self.metadata)

    def internal_kv_list(self, prefix: bytes) -> bytes:
        req = ray_client_pb2.KVListRequest(prefix=prefix)
        return self.server.KVList(req, metadata=self.metadata).keys

    def is_initialized(self) -> bool:
        if self.server is not None:
            return self.get_cluster_info(
                ray_client_pb2.ClusterInfoType.IS_INITIALIZED)
        return False


def make_client_id() -> str:
    id = uuid.uuid4()
    return id.hex


def decode_exception(data) -> Exception:
    data = base64.standard_b64decode(data)
    return loads_from_server(data)
