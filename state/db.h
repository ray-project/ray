#ifndef DB_H
#define DB_H

#include "event_loop.h"

typedef struct db_conn_impl db_conn;

/* Connect to the global system store at address and port. The last
 * parameter is an output parameter and we assume the memory is
 * allocated by the caller. */
void db_connect(const char *db_address,
                int db_port,
                const char *client_type,
                const char *client_addr,
                int client_port,
                db_conn *db);

/* Attach global system store onnection to event loop. Returns the index of the
 * connection in the loop. */
int64_t db_attach(db_conn *db, event_loop *loop, int connection_type);

/* This function will be called by the user if there is a new event in the
 * event loop associated with the global system store connection. */
void db_event(db_conn *db);

/* Disconnect from the global system store. */
void db_disconnect(db_conn *db);

#endif
