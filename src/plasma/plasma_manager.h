#ifndef PLASMA_MANAGER_H
#define PLASMA_MANAGER_H

#include <poll.h>
#include "utarray.h"

#ifndef RAY_NUM_RETRIES
#define NUM_RETRIES 5
#else
#define NUM_RETRIES RAY_NUM_RETRIES
#endif

/* Timeouts are in milliseconds. */
#ifndef RAY_TIMEOUT
#define MANAGER_TIMEOUT 1000
#else
#define MANAGER_TIMEOUT RAY_TIMEOUT
#endif

/* The buffer size in bytes. Data will get transfered in multiples of this */
#define BUFSIZE 4096

typedef struct plasma_manager_state plasma_manager_state;
typedef struct client_connection client_connection;
typedef struct client_object_request client_object_request;

/**
 * Initializes the plasma manager state. This connects the manager to the local
 * plasma store, starts the manager listening for client connections, and
 * connects the manager to a database if there is one. The returned manager
 * state should be freed using the provided destroy_plasma_manager_state
 * function.
 *
 * @param store_socket_name The socket name used to connect to the local store.
 * @param manager_socket_name The socket name used to connect to the manager.
 * @param manager_addr Our IP address.
 * @param manager_port The IP port that we listen on.
 * @param db_addr The IP address of the database to connect to. If this is NULL,
 *        then the manager will be initialized without a database
 *        connection.
 * @param db_port The IP port of the database to connect to.
 * @return A pointer to the initialized plasma manager state.
 */
plasma_manager_state *init_plasma_manager_state(const char *store_socket_name,
                                                const char *manager_socket_name,
                                                const char *manager_addr,
                                                int manager_port,
                                                const char *db_addr,
                                                int db_port);

/**
 * Destroys the plasma manager state and its connections.
 *
 * @param state A pointer to the plasma manager state to destroy.
 * @return Void.
 */
void destroy_plasma_manager_state(plasma_manager_state *state);

/**
 * Process a request from another object store manager to transfer an object.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param object_id The object_id of the object we will be sending.
 * @param addr The IP address of the plasma manager to send the object to.
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
 * Callback that will be called when a new object becomes available.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param client_sock The connection to the plasma store.
 * @param context Plasma manager state.
 * @param events (unused).
 * @return Void.
 */
void process_object_notification(event_loop *loop,
                                 int client_sock,
                                 void *context,
                                 int events);

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
 *        queue of objects that will be sent.
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
client_connection *new_client_connection(event_loop *loop,
                                         int listener_sock,
                                         void *context,
                                         int events);

/**
 * The following definitions are internal to the plasma manager code but are
 * needed by the unit tests in test/manager_tests.c. This includes structs
 * instantiated by the unit tests and forward declarations for functions used
 * internally by the plasma manager code.
 */

/* Buffer for requests between plasma managers. */
typedef struct plasma_request_buffer plasma_request_buffer;
struct plasma_request_buffer {
  int type;
  object_id object_id;
  uint8_t *data;
  int64_t data_size;
  uint8_t *metadata;
  int64_t metadata_size;
  /* Pointer to the next buffer that we will write to this plasma manager. This
   * field is only used if we're pushing requests to another plasma manager,
   * not if we are receiving data. */
  plasma_request_buffer *next;
};

/**
 * Call the request_transfer method, which well attempt to get an object from
 * a remote Plasma manager. If it is unable to get it from another Plasma
 * manager, it will cycle through a list of Plasma managers that have the
 * object.
 *
 * @param object_id The object ID of the object to transfer.
 * @param manager_count The number of managers that have the object.
 * @param manager_vector The Plasma managers that have the object.
 * @param context The plasma manager state.
 * @return Void.
 */
void call_request_transfer(object_id object_id,
                           int manager_count,
                           const char *manager_vector[],
                           void *context);

/**
 * Clean up and free an active object context. Deregister it from the
 * associated client connection and from the manager state.
 *
 * @param client_conn The client connection context.
 * @param object_id The object ID whose context we want to delete.
 * @return Void.
 */
void remove_object_request(client_connection *client_conn,
                           client_object_request *object_req);

/**
 * Get a connection to the remote manager at the specified address. Creates a
 * new connection to this manager if one doesn't already exist.
 *
 * @param state Our plasma manager state.
 * @param ip_addr The IP address of the remote manager we want to connect to.
 * @param port The port that the remote manager is listening on.
 * @return A pointer to the connection to the remote manager.
 */
client_connection *get_manager_connection(plasma_manager_state *state,
                                          const char *ip_addr,
                                          int port);

/**
 * Reads an object chunk sent by the given client into a buffer. This is the
 * complement to write_object_chunk.
 *
 * @param conn The connection to the client who's sending the data.
 * @param buf The buffer to write the data into.
 * @return An integer representing whether the client is done sending this
 *         object. 1 means that the client has sent all the data, 0 means there
 *         is more.
 */
int read_object_chunk(client_connection *conn, plasma_request_buffer *buf);

/**
 * Writes an object chunk from a buffer to the given client. This is the
 * complement to read_object_chunk.
 *
 * @param conn The connection to the client who's receiving the data.
 * @param buf The buffer to read data from.
 * @return Void.
 */
void write_object_chunk(client_connection *conn, plasma_request_buffer *buf);

/**
 * Get the event loop of the given plasma manager state.
 *
 * @param state The state of the plasma manager whose loop we want.
 * @return A pointer to the manager's event loop.
 */
event_loop *get_event_loop(plasma_manager_state *state);

/**
 * Get the file descriptor for the given client's socket. This is the socket
 * that the client sends or reads data through.
 *
 * @param conn The connection to the client who's sending or reading data.
 * @return A file descriptor for the socket.
 */
int get_client_sock(client_connection *conn);

/**
 * Return whether or not the object is local.
 *
 * @param state The state of the plasma manager.
 * @param object_id The ID of the object we want to find.
 * @return A bool that is true if the requested object is local and false
 *         otherwise.
 */
bool is_object_local(plasma_manager_state *state, object_id object_id);

#endif /* PLASMA_MANAGER_H */
