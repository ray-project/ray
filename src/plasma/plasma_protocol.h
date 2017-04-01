#ifndef PLASMA_PROTOCOL_H
#define PLASMA_PROTOCOL_H

/* C++ include files. */
#include <unordered_set>

#include "format/plasma_generated.h"

#include "common.h"
#include "plasma.h"

#include <unordered_map>

typedef void protocol_builder;

/* An argument to a function that a return value gets written to. */
#define OUT

protocol_builder *make_protocol_builder(void);

void free_protocol_builder(protocol_builder *builder);

/* Plasma receive message. */

uint8_t *plasma_receive(int sock, int64_t message_type);

/* Plasma Create message functions. */

int plasma_send_CreateRequest(int sock,
                              protocol_builder *B,
                              ObjectID object_id,
                              int64_t data_size,
                              int64_t metadata_size);

void plasma_read_CreateRequest(uint8_t *data,
                               ObjectID *object_id,
                               int64_t *data_size,
                               int64_t *metadata_size);

int plasma_send_CreateReply(int sock,
                            protocol_builder *B,
                            ObjectID object_id,
                            PlasmaObject *object,
                            int error);

void plasma_read_CreateReply(uint8_t *data,
                             ObjectID *object_id,
                             PlasmaObject *object,
                             int *error);

/* Plasma Seal message functions. */

int plasma_send_SealRequest(int sock,
                            protocol_builder *B,
                            ObjectID object_id,
                            unsigned char *digest);

void plasma_read_SealRequest(uint8_t *data,
                             OUT ObjectID *object_id,
                             OUT unsigned char *digest);

int plasma_send_SealReply(int sock,
                          protocol_builder *B,
                          ObjectID object_id,
                          int error);

void plasma_read_SealReply(uint8_t *data, ObjectID *object_id, int *error);

/* Plasma Get message functions. */

int plasma_send_GetRequest(int sock,
                           protocol_builder *B,
                           ObjectID object_ids[],
                           int64_t num_objects,
                           int64_t timeout_ms);

int64_t plasma_read_GetRequest_num_objects(uint8_t *data);

void plasma_read_GetRequest(uint8_t *data,
                            ObjectID object_ids[],
                            int64_t *timeout_ms,
                            int64_t num_objects);

int plasma_send_GetReply(int sock,
                         protocol_builder *B,
                         ObjectID object_ids[],
                         PlasmaObject plasma_objects[],
                         int64_t num_objects);

void plasma_read_GetReply(uint8_t *data,
                          ObjectID object_ids[],
                          PlasmaObject plasma_objects[],
                          int64_t num_objects);

/* Plasma Release message functions. */

int plasma_send_ReleaseRequest(int sock,
                               protocol_builder *B,
                               ObjectID object_id);

void plasma_read_ReleaseRequest(uint8_t *data, ObjectID *object_id);

int plasma_send_ReleaseReply(int sock,
                             protocol_builder *B,
                             ObjectID object_id,
                             int error);

void plasma_read_ReleaseReply(uint8_t *data, ObjectID *object_id, int *error);

/* Plasma Delete message functions. */

int plasma_send_DeleteRequest(int sock,
                              protocol_builder *B,
                              ObjectID object_id);

void plasma_read_DeleteRequest(uint8_t *data, ObjectID *object_id);

int plasma_send_DeleteReply(int sock,
                            protocol_builder *B,
                            ObjectID object_id,
                            int error);

void plasma_read_DeleteReply(uint8_t *data, ObjectID *object_id, int *error);

/* Plasma Status message functions. */

int plasma_send_StatusRequest(int sock,
                              protocol_builder *B,
                              ObjectID object_ids[],
                              int64_t num_objects);

int64_t plasma_read_StatusRequest_num_objects(uint8_t *data);

void plasma_read_StatusRequest(uint8_t *data,
                               ObjectID object_ids[],
                               int64_t num_objects);

int plasma_send_StatusReply(int sock,
                            protocol_builder *B,
                            ObjectID object_ids[],
                            int object_status[],
                            int64_t num_objects);

int64_t plasma_read_StatusReply_num_objects(uint8_t *data);

void plasma_read_StatusReply(uint8_t *data,
                             ObjectID object_ids[],
                             int object_status[],
                             int64_t num_objects);

/* Plasma Constains message functions. */

int plasma_send_ContainsRequest(int sock,
                                protocol_builder *B,
                                ObjectID object_id);

void plasma_read_ContainsRequest(uint8_t *data, ObjectID *object_id);

int plasma_send_ContainsReply(int sock,
                              protocol_builder *B,
                              ObjectID object_id,
                              int has_object);

void plasma_read_ContainsReply(uint8_t *data,
                               ObjectID *object_id,
                               int *has_object);

/* Plasma Connect message functions. */

int plasma_send_ConnectRequest(int sock, protocol_builder *B);

void plasma_read_ConnectRequest(uint8_t *data);

int plasma_send_ConnectReply(int sock,
                             protocol_builder *B,
                             int64_t memory_capacity);

void plasma_read_ConnectReply(uint8_t *data, int64_t *memory_capacity);

/* Plasma Evict message functions (no reply so far). */

int plasma_send_EvictRequest(int sock, protocol_builder *B, int64_t num_bytes);

void plasma_read_EvictRequest(uint8_t *data, int64_t *num_bytes);

int plasma_send_EvictReply(int sock, protocol_builder *B, int64_t num_bytes);

void plasma_read_EvictReply(uint8_t *data, int64_t *num_bytes);

/* Plasma Fetch Remote message functions. */

int plasma_send_FetchRequest(int sock,
                             protocol_builder *B,
                             ObjectID object_ids[],
                             int64_t num_objects);

int64_t plasma_read_FetchRequest_num_objects(uint8_t *data);

void plasma_read_FetchRequest(uint8_t *data,
                              ObjectID object_ids[],
                              int64_t num_objects);

/* Plasma Wait message functions. */

int plasma_send_WaitRequest(int sock,
                            protocol_builder *B,
                            ObjectRequest object_requests[],
                            int num_requests,
                            int num_ready_objects,
                            int64_t timeout_ms);

int plasma_read_WaitRequest_num_object_ids(uint8_t *data);

void plasma_read_WaitRequest(uint8_t *data,
                             ObjectRequest object_requests[],
                             int num_object_ids,
                             int64_t *timeout_ms,
                             int *num_ready_objects);

int plasma_send_WaitReply(int sock,
                          protocol_builder *B,
                          const std::unordered_map<ObjectID, ObjectRequest, decltype(&hashObjectID)> &object_requests,
//                          ObjectRequest object_requests[],
                          int num_ready_objects);

void plasma_read_WaitReply(uint8_t *data,
                           ObjectRequest object_requests[],
                           int *num_ready_objects);

/* Plasma Subscribe message functions. */

int plasma_send_SubscribeRequest(int sock, protocol_builder *B);

/* Plasma Data message functions. */

int plasma_send_DataRequest(int sock,
                            protocol_builder *B,
                            ObjectID object_id,
                            const char *address,
                            int port);

void plasma_read_DataRequest(uint8_t *data,
                             ObjectID *object_id,
                             char **address,
                             int *port);

int plasma_send_DataReply(int sock,
                          protocol_builder *B,
                          ObjectID object_id,
                          int64_t object_size,
                          int64_t metadata_size);

void plasma_read_DataReply(uint8_t *data,
                           ObjectID *object_id,
                           int64_t *object_size,
                           int64_t *metadata_size);

#endif /* PLASMA_PROTOCOL */
