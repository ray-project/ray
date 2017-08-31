#ifndef DB_H
#define DB_H

#include "common.h"
#include "event_loop.h"

typedef struct DBHandle DBHandle;

/**
 * Connect to the global system store.
 *
 * @param db_address The hostname to use to connect to the database.
 * @param db_port The port to use to connect to the database.
 * @param db_shards_addresses The list of database shard IP addresses.
 * @param db_shards_ports The list of database shard ports, in the same order
 *        as db_shards_addresses.
 * @param client_type The type of this client.
 * @param node_ip_address The hostname of the client that is connecting.
 * @param num_args The number of extra arguments that should be supplied. This
 *        should be an even number.
 * @param args An array of extra arguments strings. They should alternate
 *        between the name of the argument and the value of the argument. For
 *        examples: "port", "1234", "socket_name", "/tmp/s1".
 * @return This returns a handle to the database, which must be freed with
 *         db_disconnect after use.
 */
DBHandle *db_connect(const std::string &db_primary_address,
                     int db_primary_port,
                     const char *client_type,
                     const char *node_ip_address,
                     int num_args,
                     const char **args);

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
void db_attach(DBHandle *db, event_loop *loop, bool reattach);

/**
 * Disconnect from the global system store.
 *
 * @param db The database connection to close and clean up.
 * @return Void.
 */
void db_disconnect(DBHandle *db);

/**
 * Free the database handle.
 *
 * @param db The database connection to clean up.
 * @return Void.
 */
void DBHandle_free(DBHandle *db);

/**
 * Returns the db client ID.
 *
 * @param db The handle to the database.
 * @returns int The db client ID for this connection to the database.
 */
DBClientID get_db_client_id(DBHandle *db);

#endif
