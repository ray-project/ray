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

/* Check if the object store contains a particular object and the object has
 * been sealed. The result will be stored in has_object. TODO(rkn): We may want
 * to indicate whether the object is currently being written. */
void plasma_contains(plasma_store_conn *conn,
                     plasma_id object_id,
                     int *has_object);

void plasma_seal(plasma_store_conn *conn, plasma_id object_id);

/* Delete an object from the object store. This currently assumes that the
 * object is present and has been sealed. TODO(rkn): We may want to allow the
 * deletion of objects that are not present or haven't been sealed. */
void plasma_delete(plasma_store_conn *conn, plasma_id object_id);

#endif
