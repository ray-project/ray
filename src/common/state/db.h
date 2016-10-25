#ifndef DB_H
#define DB_H

#include "event_loop.h"

typedef struct db_handle_impl db_handle;

/* Connect to the global system store at address and port. Returns
 * a handle to the database, which must be freed with db_disconnect
 * after use. */
db_handle *db_connect(const char *db_address,
                      int db_port,
                      const char *client_type,
                      const char *client_addr,
                      int client_port);

/* Attach global system store connection to event loop. */
void db_attach(db_handle *db, event_loop *loop);

/* Disconnect from the global system store. */
void db_disconnect(db_handle *db);

/**
 * Returns the client ID, according to the database.
 *
 * @param db The handle to the database.
 * @returns int The client ID for this connection to the database. If
 *          this client has no connection to the database, returns -1.
 */
int get_client_id(db_handle *db);

#endif
