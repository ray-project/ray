#ifndef PLASMA_CLIENT_H
#define PLASMA_CLIENT_H

/* Connect to the local plasma store UNIX domain socket with path socket_name
 * and return the resulting connection. */
plasma_store_conn *plasma_store_connect(const char *socket_name);

/* Connect to a possibly remote plasma manager */
int plasma_manager_connect(const char *addr, int port);

void plasma_create(plasma_store_conn *conn,
                   plasma_id object_id,
                   int64_t size,
                   uint8_t *metadata,
                   int64_t metadata_size,
                   uint8_t **data);

void plasma_get(plasma_store_conn *conn,
                plasma_id object_id,
                int64_t *size,
                uint8_t **data,
                int64_t *metadata_size,
                uint8_t **metadata);

void plasma_seal(plasma_store_conn *conn, plasma_id object_id);

#endif
