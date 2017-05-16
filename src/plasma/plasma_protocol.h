#ifndef PLASMA_PROTOCOL_H
#define PLASMA_PROTOCOL_H

#include "status.h"
#include "format/plasma_generated.h"
#include "plasma.h"

using arrow::Status;

/* Plasma receive message. */

Status PlasmaReceive(int sock, int64_t message_type, std::vector<uint8_t>& buffer);

/* Plasma Create message functions. */

Status SendCreateRequest(int sock,
                      ObjectID object_id,
                      int64_t data_size,
                      int64_t metadata_size);

Status ReadCreateRequest(uint8_t *data,
                       ObjectID *object_id,
                       int64_t *data_size,
                       int64_t *metadata_size);

Status SendCreateReply(int sock,
                    ObjectID object_id,
                    PlasmaObject *object,
                    int error);

Status ReadCreateReply(uint8_t *data,
                       ObjectID *object_id,
                       PlasmaObject *object);

/* Plasma Seal message functions. */

Status SendSealRequest(int sock, ObjectID object_id, unsigned char *digest);

Status ReadSealRequest(uint8_t *data, ObjectID *object_id, unsigned char *digest);

Status SendSealReply(int sock, ObjectID object_id, int error);

Status ReadSealReply(uint8_t *data, ObjectID *object_id);

/* Plasma Get message functions. */

Status SendGetRequest(int sock,
                   ObjectID object_ids[],
                   int64_t num_objects,
                   int64_t timeout_ms);

Status ReadGetRequest(uint8_t *data,
                    std::vector<ObjectID>& object_ids,
                    int64_t *timeout_ms);

Status SendGetReply(
    int sock,
    ObjectID object_ids[],
    std::unordered_map<ObjectID, PlasmaObject, UniqueIDHasher> &plasma_objects,
    int64_t num_objects);

Status ReadGetReply(uint8_t *data,
                  ObjectID object_ids[],
                  PlasmaObject plasma_objects[],
                  int64_t num_objects);

/* Plasma Release message functions. */

Status SendReleaseRequest(int sock, ObjectID object_id);

Status ReadReleaseRequest(uint8_t *data, ObjectID *object_id);

Status SendReleaseReply(int sock, ObjectID object_id, int error);

Status ReadReleaseReply(uint8_t *data, ObjectID *object_id);

/* Plasma Delete message functions. */

Status SendDeleteRequest(int sock, ObjectID object_id);

Status ReadDeleteRequest(uint8_t *data, ObjectID *object_id);

Status SendDeleteReply(int sock, ObjectID object_id, int error);

Status ReadDeleteReply(uint8_t *data, ObjectID *object_id);

/* Satus messages. */

Status SendStatusRequest(int sock,
                              ObjectID object_ids[],
                              int64_t num_objects);

int64_t ReadStatusRequest_num_objects(uint8_t *data);

Status ReadStatusRequest(uint8_t *data,
                               ObjectID object_ids[],
                               int64_t num_objects);

Status SendStatusReply(int sock,
                            ObjectID object_ids[],
                            int object_status[],
                            int64_t num_objects);

int64_t ReadStatusReply_num_objects(uint8_t *data);

Status ReadStatusReply(uint8_t *data,
                             ObjectID object_ids[],
                             int object_status[],
                             int64_t num_objects);

/* Plasma Constains message functions. */

Status SendContainsRequest(int sock, ObjectID object_id);

Status ReadContainsRequest(uint8_t *data, ObjectID *object_id);

Status SendContainsReply(int sock, ObjectID object_id, int has_object);

Status ReadContainsReply(uint8_t *data, ObjectID *object_id, int *has_object);

/* Plasma Connect message functions. */

Status SendConnectRequest(int sock);

Status ReadConnectRequest(uint8_t *data);

Status SendConnectReply(int sock, int64_t memory_capacity);

Status ReadConnectReply(uint8_t *data, int64_t *memory_capacity);

/* Plasma Evict message functions (no reply so far). */

Status SendEvictRequest(int sock, int64_t num_bytes);

Status ReadEvictRequest(uint8_t *data, int64_t *num_bytes);

Status SendEvictReply(int sock, int64_t num_bytes);

Status ReadEvictReply(uint8_t *data, int64_t &num_bytes);

/* Plasma Fetch Remote message functions. */

Status SendFetchRequest(int sock,
                             ObjectID object_ids[],
                             int64_t num_objects);

int64_t ReadFetchRequest_num_objects(uint8_t *data);

Status ReadFetchRequest(uint8_t *data,
                              ObjectID object_ids[],
                              int64_t num_objects);

/* Plasma Wait message functions. */

Status SendWaitRequest(int sock,
                            ObjectRequest object_requests[],
                            int num_requests,
                            int num_ready_objects,
                            int64_t timeout_ms);

int ReadWaitRequest_num_object_ids(uint8_t *data);

Status ReadWaitRequest(uint8_t *data,
                             ObjectRequestMap &object_requests,
                             int num_object_ids,
                             int64_t *timeout_ms,
                             int *num_ready_objects);

Status SendWaitReply(int sock,
                          const ObjectRequestMap &object_requests,
                          int num_ready_objects);

Status ReadWaitReply(uint8_t *data,
                           ObjectRequest object_requests[],
                           int *num_ready_objects);

/* Plasma Subscribe message functions. */

Status SendSubscribeRequest(int sock);

/* Data messages. */

Status SendDataRequest(int sock,
                    ObjectID object_id,
                    const char *address,
                    int port);

Status ReadDataRequest(uint8_t *data,
                     ObjectID *object_id,
                     char **address,
                     int *port);

Status SendDataReply(int sock,
                  ObjectID object_id,
                  int64_t object_size,
                  int64_t metadata_size);

Status ReadDataReply(uint8_t *data,
                   ObjectID *object_id,
                   int64_t *object_size,
                   int64_t *metadata_size);

#endif /* PLASMA_PROTOCOL */
