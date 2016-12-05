#ifndef DB_H
#define DB_H

#include "common.h"
#include "event_loop.h"

typedef struct db_handle db_handle;

/**
 * Connect to the global system store.
 *
 * @param db_address The hostname to use to connect to the database.
 * @param db_port The port to use to connect to the database.
 * @param client_type The type of this client.
 * @param client_addr The hostname of the client that is connecting. If not
 *        relevant, set this to the empty string.
 * @param client_port The port of the client that is connecting. If not
 *        relevant, set this to -1.
 * @return This returns a handle to the database, which must be freed with
 *         db_disconnect after use.
 */

db_handle *db_connect(const char *db_address,
                      int db_port,
                      const char *client_type,
                      const char *client_addr,
                      int client_port);

/**
 * Attach global system store connection to an event loop. Callbacks from
 * queries to the global system store will trigger events in the event loop.
 *
 * @param db The handle to the database that is connected.
 * @param loop The event loop the database gets connected to.
 * @param reattach Can only be true in unit tests. If true, the database is
 *        reattached to the loop.
 * @return Void.
 */
void db_attach(db_handle *db, event_loop *loop, bool reattach);

/**
 * Disconnect from the global system store.
 *
 * @param db The database connection to close and clean up.
 * @return Void.
 */
void db_disconnect(db_handle *db);

/**
 * Returns the db client ID.
 *
 * @param db The handle to the database.
 * @returns int The db client ID for this connection to the database.
 */
db_client_id get_db_client_id(db_handle *db);

#endif
