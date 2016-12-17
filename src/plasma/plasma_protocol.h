#ifndef PLASMA_PROTOCOL_H
#define PLASMA_PROTOCOL_H

#define PLASMA_PROTOCOL_VERSION 0x0000000000000000

#include "common.h"
#include "plasma.h"

#include "format/plasma_builder.h"
#include "format/plasma_reader.h"

/* An argument to a function that a return value gets written to. */
#define OUT

/* Protocol builder. */

typedef flatcc_builder_t protocol_builder;

protocol_builder *make_protocol_builder(void);

void free_protocol_builder(protocol_builder *builder);

/* PlasmaCreate message functions. */

int plasma_send_CreateRequest(int sock,
                              protocol_builder *B,
                              object_id object_id,
                              int64_t data_size,
                              int64_t metadata_size);

void plasma_read_CreateRequest(uint8_t *data,
                               object_id *object_id,
                               int64_t *data_size,
                               int64_t *metadata_size);

int plasma_send_CreateReply(int sock,
                            protocol_builder *B,
                            object_id object_id,
                            plasma_object *object,
                            int error);

void plasma_read_CreateReply(uint8_t *data,
                             object_id *object_id,
                             plasma_object *object,
                             int *error);

/* Plasma Seal message functions. */

int plasma_send_SealRequest(int sock, protocol_builder *B, object_id object_id, unsigned char *digest);

void plasma_read_SealRequest(uint8_t *data, OUT object_id *object_id, OUT unsigned char *digest);

int plasma_send_SealReply(int sock, protocol_builder *B, object_id object_id, int error);

void plasma_read_SealReply(uint8_t *data, object_id *object_id, int *error);

/* Plasma Get message functions. */

int plasma_send_GetRequest(int sock,
                           protocol_builder *B,
                           object_id object_ids[],
                           int64_t num_objects);

void plasma_read_GetRequest(uint8_t *data,
                            object_id** object_ids_ptr,
                            int64_t *num_objects);

int plasma_send_GetReply(int sock,
                         protocol_builder *B,
                         object_id object_ids[],
                         plasma_object plasma_objects[],
                         int64_t num_objects);

void plasma_read_GetReply(uint8_t *data,
                          object_id** object_ids_ptr,
                          plasma_object plasma_objects[],
                          int64_t *num_objects);

/* Plasma Release message functions. */

int plasma_send_ReleaseRequest(int sock, protocol_builder *B, object_id object_id);

void plasma_read_ReleaseRequest(uint8_t *data, object_id *object_id);

int plasma_send_ReleaseReply(int sock, protocol_builder *B, object_id object_id, int error);

void plasma_read_ReleaseReply(uint8_t *data, object_id *object_id, int *error);

/* Plasma Delete message functions. */

int plasma_send_DeleteRequest(int sock, protocol_builder *B, object_id object_id);

void plasma_read_DeleteRequest(uint8_t *data, object_id *object_id);

int plasma_send_DeleteReply(int sock, protocol_builder *B, object_id object_id, int error);

void plasma_read_DeleteReply(uint8_t *data, object_id *object_id, int *error);

/* Plasma Evict message functions (no reply so far). */

int plasma_send_EvictRequest(int sock, protocol_builder *B, int64_t num_bytes);

void plasma_read_EvictRequest(uint8_t *data, int64_t *num_bytes);

int plasma_send_EvictReply(int sock, protocol_builder *B, int64_t num_bytes);

void plasma_read_EvictReply(uint8_t *data, int64_t *num_bytes);

/* Plasma Fetch Remote message functions. */

int plasma_send_FetchRequest(int sock,
                           protocol_builder *B,
                           object_id object_ids[],
                           int64_t num_objects);

void plasma_read_FetchRequest(uint8_t *data,
                              object_id** object_ids_ptr,
                              int64_t *num_objects);

/* Plasma Wait message functions. */

int plasma_send_WaitRequest(int sock,
                            protocol_builder *B,
                            object_request object_requests[],
                            int num_requests,
                            int num_ready_objects,
                            int64_t timeout_ms);

int plasma_read_WaitRequest_num_object_ids(uint8_t *data);

void plasma_read_WaitRequest(uint8_t *data,
                             object_request object_requests[],
                             int num_object_ids,
                             int64_t *timeout_ms,
                             int *num_ready_objects);

int plasma_send_WaitReply(int sock,
                          protocol_builder *B,
                          object_request object_requests[],
                          int num_ready_objects);

void plasma_read_WaitReply(uint8_t *data,
                           object_request object_requests[],
                           int *num_ready_objects);

/* Plasma Subscribe message functions. */

int plasma_send_SubscribeRequest(int sock, protocol_builder* B);

#endif /* PLASMA_PROTOCOL */
