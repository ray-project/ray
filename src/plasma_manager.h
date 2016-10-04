#ifndef PLASMA_MANAGER_H
#define PLASMA_MANAGER_H

#include <poll.h>
#include "utarray.h"

typedef struct client_connection client_connection;

/**
 * Start transfering data to another object store manager.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param object_id The object_id of the object we will be sending.
 * @param addr The IP address of the plasma manager we are sending the object
 * to.
 * @param port The port of the plasma manager we are sending the object to.
 * @param conn The client_connection to the other plasma manager.
 *
 * This establishes a connection to the remote manager and sends the data
 * header to the other object manager.
 */
void start_writing_data(event_loop *loop,
                        object_id object_id,
                        uint8_t addr[4],
                        int port,
                        client_connection *conn);

/**
 * Start reading data from another object manager.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param client_sock The connection to the other plasma manager.
 * @param object_id The object_id of the object we will be reading.
 * @param data_size Size of the object.
 * @param metadata_size Size of the metadata.
 * @param conn The client_connection to the other plasma manager.
 *
 * Initializes the object we are going to write to in the
 * local plasma store and then switches the data socket to reading mode.
 */
void start_reading_data(event_loop *loop,
                        int client_sock,
                        object_id object_id,
                        int64_t data_size,
                        int64_t metadata_size,
                        client_connection *conn);

/**
 * Read the next chunk of the object in transit from the plasma manager
 * that is connected to the connection with index "conn_index". Once all data
 * has been read, the socket switches to listening for the next request.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param data_sock The connection to the other plasma manager.
 * @param context The client_connection to the other plasma manager.
 *
 */
void read_object_chunk(event_loop *loop,
                       int data_sock,
                       void *context,
                       int events);

/**
 * Write the next chunk of the object currently transfered to the plasma manager
 * that is connected to the socket "data_sock". If no data has been sent yet,
 * the initial handshake to transfer the object size is performed.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param data_sock This is the socket the other plasma manager is listening on.
 * @param context The client_connection to the other plasma manager, contains a
 *                queue of objects that will be sent.
 */
void write_object_chunk(event_loop *loop,
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
 *
 */
void new_client_connection(event_loop *loop,
                           int listener_sock,
                           void *context,
                           int events);

/* The buffer size in bytes. Data will get transfered in multiples of this */
#define BUFSIZE 4096

#endif /* PLASMA_MANAGER_H */
