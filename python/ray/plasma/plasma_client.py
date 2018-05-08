import asyncio
import ray

from ray.plasma.format.ObjectStatus import ObjectStatus
from ray.plasma.format.MessageType import MessageType
from ray.plasma.protocol import SendConnectRequest, PlasmaReceive, ReadConnectReply, SendWaitRequest, ReadWaitReply, \
    ObjectRequest

from ray.plasma.io import AsyncPlasmaSockets

import pyarrow.plasma as plasma

# Query for object in the local plasma store.
PLASMA_QUERY_LOCAL = 1  # plasma::PLASMA_QUERY_LOCAL
# Query for object in the local plasma store or in a remote plasma store.
PLASMA_QUERY_ANYWHERE = 2  # plasma::PLASMA_QUERY_ANYWHERE
PLASMA_WAIT_TIMEOUT = 2 ** 30


class AsyncPlasmaClient:
    """
    Imitate arrow/python/pyarrow/plasma.pyx:connect

    Attributes:
        store_socket_name (str): Name of the socket the plasma store is listening at.
        manager_socket_name (str):  Name of the socket the plasma manager is listening at.
        release_delay (int):  The maximum number of objects that the client will keep and
            delay releasing (for caching reasons).
        num_retries (int): Number of times to try to connect to plasma store. Default value of -1
            uses the default (50)
    """
    
    def __init__(self, store_socket_name, manager_socket_name='', release_delay=64, num_retries=-1, use_raylet=False):
        
        self.store_socket_name = store_socket_name
        self.manager_socket_name = manager_socket_name
        self.release_delay = release_delay
        self.num_retries = num_retries
        self.use_raylet = use_raylet
        
        self.store_capacity = None
        # Create an object store client.
        
        self.mgr_socket = None
        self.sto_socket = None
        
        if not use_raylet:
            self.plasma_client = plasma.connect(store_socket_name, manager_socket_name, 64)
        else:
            self.plasma_client = plasma.connect(store_socket_name, "", 64)
    
    def __getattr__(self, item):
        if item in self.__dict__:
            return self.__dict__[item]
        else:
            return getattr(self.plasma_client, item)
    
    async def connect(self):
        self.sto_socket = AsyncPlasmaSockets(self.store_socket_name)
        self.mgr_socket = AsyncPlasmaSockets(self.manager_socket_name)
        
        async with self.sto_socket.get_socket() as (reader, writer):
            await SendConnectRequest(writer)
            buffer = await PlasmaReceive(reader, MessageType.PlasmaConnectReply)
        
        store_capacity = ReadConnectReply(buffer)
        self.store_capacity = store_capacity
    
    async def wait(self, object_ids, timeout=PLASMA_WAIT_TIMEOUT, num_returns=-1):
        # Check that the object ID arguments are unique. The plasma manager
        # currently crashes if given duplicate object IDs.
        if len(object_ids) != len(set(object_ids)):
            raise Exception("Wait requires a list of unique object IDs.")
        
        num_object_requests = len(object_ids)
        
        object_requests = []  # [CObjectRequest] * num_object_requests
        
        for object_id in object_ids:
            object_requests.append(ObjectRequest(object_id=object_id.binary(), type=PLASMA_QUERY_ANYWHERE))
        
        #### follows PlasmaClient::Impl::Wait ###
        
        async with self.mgr_socket.get_socket() as (reader, writer):
            await SendWaitRequest(writer, object_requests, num_object_requests, num_ready_objects=num_returns,
                                  timeout_ms=timeout)
            buffer = await PlasmaReceive(reader, MessageType.PlasmaWaitReply)
        num_returns = ReadWaitReply(buffer, object_requests)
        
        num_objects_ready = 0
        
        # for i in range(num_object_requests):
        #     assert object_requests[i].type == PLASMA_QUERY_LOCAL or object_requests[i].type == PLASMA_QUERY_ANYWHERE
        
        for i in range(num_object_requests):
            rtype = object_requests[i].type
            status = object_requests[i].status
            if rtype == PLASMA_QUERY_LOCAL and status == ObjectStatus.Local:
                num_objects_ready += 1
            elif rtype == PLASMA_QUERY_ANYWHERE:
                if status == ObjectStatus.Local or status == ObjectStatus.Remote:
                    num_objects_ready += 1
                else:
                    # origin: ARROW_CHECK(status == ObjectStatus_Nonexistent);
                    assert status == ObjectStatus.Nonexistent or status == ObjectStatus.Transfer
            else:
                raise Exception("This code should be unreachable.")
        
        #### end of PlasmaClient::Impl::Wait ####
        
        num_to_return = min(num_objects_ready, num_returns)
        ready_ids = []
        waiting_ids = set(object_ids)
        
        num_returned = 0
        for i in range(len(object_ids)):
            if num_returned == num_to_return:
                break
            
            if object_requests[i].status in (ObjectStatus.Local, ObjectStatus.Remote):
                ready_ids.append(object_requests[i].object_id)
                waiting_ids.discard(object_requests[i].object_id)
                num_returned += 1
        
        return ready_ids, list(waiting_ids)
    
    def disconnect(self):
        if self.mgr_socket is not None:
            self.mgr_socket.shutdown()
            self.mgr_socket = None
        
        if self.sto_socket is not None:
            self.sto_socket.shutdown()
            self.sto_socket = None
        
        self.plasma_client.disconnect()
    
    @staticmethod
    def _ray2plasma_id(object_ids):
        return [plasma.ObjectID(object_id.id()) for object_id in object_ids]
    
    @staticmethod
    def _plasma2ray_id(object_ids):
        return [ray.local_scheduler.ObjectID(object_id.binary()) for object_id in object_ids]
    
    def wrap_objectid_with_future(self, object_ids):
        from ray.local_scheduler import ObjectID
        _self = self
        
        class ObjectIDWithFuture(asyncio.Task):
            def __init__(self, object_id):
                if not isinstance(object_id, ObjectID):
                    raise TypeError("object_id should be an instance of ObjectID")
                
                self.object_id = object_id
                
                async def _get():
                    # wait until ready
                    plasma_ids = AsyncPlasmaClient._ray2plasma_id([object_id])
                    ready_ids = (await _self.wait(plasma_ids, num_returns=1))[0]
                    ray_object_id = AsyncPlasmaClient._plasma2ray_id(ready_ids)[0]
                    return ray.get(ray_object_id)
                
                super().__init__(coro=_get(), loop=self._loop)
            
            def __getattr__(self, item):
                if item in self.__dict__:
                    return self.__dict__[item]
                else:
                    return getattr(self.object_id, item)
        
        if isinstance(object_ids, list):
            if isinstance(object_ids[0], plasma.ObjectID):
                object_ids = self._plasma2ray_id(object_ids)
            wrapper_object_ids = [ObjectIDWithFuture(object_id) for object_id in object_ids]
            return wrapper_object_ids
        elif isinstance(object_ids, ObjectID):
            return ObjectIDWithFuture(object_ids)
        elif isinstance(object_ids, plasma.ObjectID):
            ray_object_id = self._plasma2ray_id([object_ids])[0]
            return ObjectIDWithFuture(ray_object_id)
        else:
            raise TypeError("Not a supported type")
