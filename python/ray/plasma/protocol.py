# // Send a ConnectRequest to the store to get its memory capacity.
#   RETURN_NOT_OK(SendConnectRequest(store_conn_));
#   std::vector<uint8_t> buffer;
#   RETURN_NOT_OK(PlasmaReceive(store_conn_, MessageType_PlasmaConnectReply, &buffer));
#   RETURN_NOT_OK(ReadConnectReply(buffer.data(), buffer.size(), &store_capacity_));

from asyncio.streams import StreamReader, StreamWriter
import flatbuffers

import pyarrow

from ray.plasma.format.MessageType import MessageType
from ray.plasma.format import (
    PlasmaConnectRequest, PlasmaConnectReply,
    ObjectRequestSpec, ObjectReply,
    PlasmaWaitRequest, PlasmaWaitReply,
    PlasmaFetchRequest
)

from ray.plasma.io import WriteMessage, ReadMessage


class ObjectRequest:
    def __init__(self, object_id: bytes, type, status=None):
        self.object_id = pyarrow.plasma.ObjectID(object_id)
        self.type = type
        self.status = status
    
    def update(self, req: ObjectReply):
        self.object_id = pyarrow.plasma.ObjectID(req.ObjectId())
        self.status = req.Status()


def to_flatbuffer(fbb, starter, object_ids):
    specs = [fbb.CreateString(object_id.binary()) for object_id in object_ids]
    
    starter(fbb, len(object_ids))
    # Note: Since we prepend the data, prepend the items in reverse order.
    for spec in reversed(specs):
        fbb.PrependSOffsetTRelative(spec)
    object_request_specs = fbb.EndVector(len(object_ids))
    return object_request_specs


async def PlasmaSend(sock: StreamWriter, message_type, fbb, message):
    fbb.Finish(message)
    buf = fbb.Output()
    await WriteMessage(sock, message_type, len(buf), buf)


async def PlasmaReceive(sock: StreamReader, message_type):
    msg_type, buffer = await ReadMessage(sock)
    assert message_type == msg_type
    return buffer


async def SendConnectRequest(sock):
    builder = flatbuffers.Builder(0)
    PlasmaConnectRequest.PlasmaConnectRequestStart(builder)
    end = PlasmaConnectRequest.PlasmaConnectRequestEnd(builder)
    await PlasmaSend(sock, MessageType.PlasmaConnectRequest, builder, message=end)


def ReadConnectReply(data: bytes):
    message = PlasmaConnectReply.PlasmaConnectReply.GetRootAsPlasmaConnectReply(data, 0)
    memory_capacity = message.MemoryCapacity()
    return memory_capacity


def _create_object_requests(fbb, starter, object_requests):
    specs = []
    for item in object_requests:
        obj_str = fbb.CreateString(item.object_id.binary())
        ObjectRequestSpec.ObjectRequestSpecStart(fbb)
        ObjectRequestSpec.ObjectRequestSpecAddObjectId(fbb, obj_str)
        ObjectRequestSpec.ObjectRequestSpecAddType(fbb, item.type)
        spec = ObjectRequestSpec.ObjectRequestSpecEnd(fbb)
        specs.append(spec)
    
    starter(fbb, len(object_requests))
    # Note: Since we prepend the data, prepend the items in reverse order.
    for spec in reversed(specs):
        fbb.PrependSOffsetTRelative(spec)
    object_request_specs = fbb.EndVector(len(object_requests))
    return object_request_specs


async def SendWaitRequest(sock, object_requests: list, num_requests: int, num_ready_objects: int, timeout_ms: int):
    # int64_t num_requests
    # int num_ready_objects
    # int64_t timeout_ms
    assert num_requests == len(object_requests)
    # assert num_ready_objects > 0
    
    fbb = flatbuffers.Builder(0)
    
    object_request_specs = _create_object_requests(fbb,
                                                   PlasmaWaitRequest.PlasmaWaitRequestStartObjectRequestsVector,
                                                   object_requests)
    
    PlasmaWaitRequest.PlasmaWaitRequestStart(fbb)
    PlasmaWaitRequest.PlasmaWaitRequestAddObjectRequests(fbb, object_request_specs)
    PlasmaWaitRequest.PlasmaWaitRequestAddNumReadyObjects(fbb, num_ready_objects)
    PlasmaWaitRequest.PlasmaWaitRequestAddTimeout(fbb, timeout_ms)
    
    end = PlasmaWaitRequest.PlasmaWaitRequestEnd(fbb)
    
    await PlasmaSend(sock, MessageType.PlasmaWaitRequest, fbb, message=end)


def ReadWaitReply(data: bytes, object_requests: list):
    message = PlasmaWaitReply.PlasmaWaitReply.GetRootAsPlasmaWaitReply(data, 0)
    num_ready_objects = message.NumReadyObjects()
    for i in range(num_ready_objects):
        req = message.ObjectRequests(i)
        object_requests[i].update(req)
    return num_ready_objects


async def SendFetchRequest(sock, object_ids):
    fbb = flatbuffers.Builder(0)
    obj_vector = to_flatbuffer(fbb, PlasmaFetchRequest.PlasmaFetchRequestStartObjectIdsVector, object_ids)
    
    PlasmaFetchRequest.PlasmaFetchRequestStart(fbb)
    PlasmaFetchRequest.PlasmaFetchRequestAddObjectIds(fbb, obj_vector)
    message = PlasmaFetchRequest.PlasmaFetchRequestEnd(fbb)
    return PlasmaSend(sock, MessageType.PlasmaFetchRequest, fbb, message)
