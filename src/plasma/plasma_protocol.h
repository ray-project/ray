#ifndef PLASMA_PROTOCOL_H
#define PLASMA_PROTOCOL_H

#define PLASMA_PROTOCOL_VERSION 0x0000000000000000

#include "common.h"
#include "plasma.h"

#include "format/plasma_reader.h"

int plasma_send_create_request(int sock,
                               object_id object_id,
                               int64_t data_size,
                               int64_t metadata_size);

void plasma_read_create_request(uint8_t *data,
                                object_id *object_id,
                                int64_t *data_size,
                                int64_t *metadata_size);

int plasma_send_create_reply(int sock,
                             object_id object_id,
                             plasma_object *object,
                             int error_code);

void plasma_read_create_reply(uint8_t *data,
                              object_id *object_id,
                              plasma_object *object,
                              int *error_code);

#endif /* PLASMA_PROTOCOL */
