#ifndef PLASMA_MANAGER_H
#define PLASMA_MANAGER_H

#include <poll.h>
#include "utarray.h"

typedef struct client_connection client_connection;

/**
 * Process a request from another object store manager to transfer an object.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param object_id The object_id of the object we will be sending.
 * @param addr The IP address of the plasma manager we are sending the object
 * to.
 * @param port The port of the plasma manager we are sending the object to.
 * @param conn The client_connection to the other plasma manager.
 * @return Void.
 *
 * This establishes a connection to the remote manager if one doesn't already
 * exist, and queues up the request to transfer the data to the other object
 * manager.
 */
void process_transfer(event_loop *loop,
                      object_id object_id,
                      uint8_t addr[4],
                      int port,
                      client_connection *conn);

/**
 * Process a request from another object store manager to receive data.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param client_sock The connection to the other plasma manager.
 * @param object_id The object_id of the object we will be reading.
 * @param data_size Size of the object.
 * @param metadata_size Size of the metadata.
 * @param conn The client_connection to the other plasma manager.
 * @return Void.
 *
 * Initializes the object we are going to write to in the local plasma store
 * and then switches the data socket to read the raw object bytes instead of
 * plasma requests.
 */
void process_data(event_loop *loop,
                  int client_sock,
                  object_id object_id,
                  int64_t data_size,
                  int64_t metadata_size,
                  client_connection *conn);

/**
 * Read the next chunk of the object in transit from the plasma manager
 * connected to the given socket. Once all data for this object has been read,
 * the socket switches to listening for the next plasma request.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param data_sock The connection to the other plasma manager.
 * @param context The client_connection to the other plasma manager.
 * @return Void.
 */
void process_data_chunk(event_loop *loop,
                        int data_sock,
                        void *context,
                        int events);

/**
 * Process a fetch request. The fetch request tries:
 * 1) If there is no connection to the database, return faliure to the client.
 * 2) If the object is available locally, return success to the client.
 * 3) Query the database for plasma managers that the object might be on.
 * 4) Request a transfer from each of the managers that the object might be on
 *    until we receive the data, or until we timeout.
 * 5) Returns success or failure to the client depending on whether we received
 *    the data or not.
 *
 * @param client_conn The connection context for the client that made the
 *        request.
 * @param object_id The object ID requested.
 * @return Void.
 */
void process_fetch_request(client_connection *client_conn, object_id object_id);

/**
 * Process a fetch request for multiple objects. The success of each object
 * will be written back individually to the socket connected to the client that
 * made the request in a plasma_reply. See documentation for
 * process_fetch_request for the sequence of operations per object.
 *
 * @param client_conn The connection context for the client that made the
 *        request.
 * @param object_id_count The number of object IDs requested.
 * @param object_ids[] The vector of object IDs requested.
 * @return Void.
 */
void process_fetch_requests(client_connection *client_conn,
                            int object_id_count,
                            object_id object_ids[]);

/**
 * Send the next request queued for the other plasma manager connected to the
 * socket "data_sock". This could be a request to either write object data or
 * request object data. If the request is to write object data and no data has
 * been sent yet, the initial handshake to transfer the object size is
 * performed.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param data_sock This is the socket the other plasma manager is listening on.
 * @param context The client_connection to the other plasma manager, contains a
 *                queue of objects that will be sent.
 * @return Void.
 */
void send_queued_request(event_loop *loop,
                         int data_sock,
                         void *context,
                         int events);

/**
 * Register a new client connection with the plasma manager. A client can
 * either be a worker or another plasma manager.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param listener_socket The socket the plasma manager is listening on.
 * @param context The plasma manager state.
 * @return Void.
 */
void new_client_connection(event_loop *loop,
                           int listener_sock,
                           void *context,
                           int events);

/* The buffer size in bytes. Data will get transfered in multiples of this */
#define BUFSIZE 4096

#endif /* PLASMA_MANAGER_H */
