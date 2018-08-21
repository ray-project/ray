#ifndef PLASMA_PROTOCOL_H
#define PLASMA_PROTOCOL_H

#include "./format/common_generated.h"
#include "./format/plasma_generated.h"

namespace plasma {

namespace flatbuf {
enum class MessageType : int64_t;
};

using arrow::Status;

typedef std::unordered_map<ObjectID, ObjectRequest> ObjectRequestMap;

Status PlasmaReceive(int sock,
                     flatbuf::MessageType message_type,
                     std::vector<uint8_t> *buffer);

Status SendWaitReply(int sock,
                     const ObjectRequestMap &object_requests,
                     int num_ready_objects);

Status SendStatusReply(int sock,
                       ObjectID object_ids[],
                       int object_status[],
                       int64_t num_objects);

Status SendDataRequest(int sock,
                       ObjectID object_id,
                       const char *address,
                       int port);

Status SendDataReply(int sock,
                     ObjectID object_id,
                     int64_t object_size,
                     int64_t metadata_size);

Status ReadDataRequest(uint8_t *data,
                       size_t size,
                       ObjectID *object_id,
                       char **address,
                       int *port);

Status ReadDataReply(uint8_t *data,
                     size_t size,
                     ObjectID *object_id,
                     int64_t *object_size,
                     int64_t *metadata_size);

Status ReadFetchRequest(uint8_t *data,
                        size_t size,
                        std::vector<ObjectID> &object_ids);

Status ReadStatusRequest(uint8_t *data,
                         size_t size,
                         ObjectID object_ids[],
                         int64_t num_objects);

Status ReadWaitRequest(uint8_t *data,
                       size_t size,
                       ObjectRequestMap &object_requests,
                       int64_t *timeout_ms,
                       int *num_ready_objects);

Status ReadStatusRequest(uint8_t *data,
                         size_t size,
                         ObjectID object_ids[],
                         int64_t num_objects);

std::unique_ptr<uint8_t[]> CreateObjectInfoBuffer(
    flatbuf::ObjectInfoT *object_info);

}  // namespace plasma

#endif
