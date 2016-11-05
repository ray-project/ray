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
 * @param manager_addr Our IP address.
 * @param manager_port The IP port that we listen on.
 * @param db_addr The IP address of the database to connect to. If this is
 *        NULL, then the manager will be initialized without a database
 *        connection.
 * @param db_port The IP port of the database to connect to.
 * @return A pointer to the initialized plasma manager state.
 */
plasma_manager_state *init_plasma_manager_state(const char *store_socket_name,
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
 * @param num_object_ids The number of object IDs requested.
 * @param object_ids[] The vector of object IDs requested.
 * @return Void.
 */
void process_fetch_requests(client_connection *client_conn,
                            int num_object_ids,
                            object_id object_ids[]);

/**
 * Process a wait request from a client.
 *
 * @param client_conn The connection context for the client that made the
 *        request.
 * @param num_object_ids Number of object IDs wait is called on.
 * @param object_ids Object IDs wait is called on.
 * @param timeout Wait will time out and return after this number of
 *        milliseconds.
 * @param num_returns Number of object IDs wait will return if it doesn't time
 *        out.
 * @return Void.
 */
void process_wait_request(client_connection *client_conn,
                          int num_object_ids,
                          object_id object_ids[],
                          uint64_t timeout,
                          int num_returns);

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
 * Create a new context for the given object ID with the given
 * client connection and register it with the manager's
 * outstanding fetch or wait requests and the client
 * connection's active object contexts.
 *
 * @param client_conn The client connection context.
 * @param object_id The object ID whose context we want to
 *        create.
 * @return A pointer to the newly created object context.
 */
client_object_request *add_object_request(client_connection *client_conn,
                                          object_id object_id);

/**
 * Given an object ID and the managers it can be found on, start requesting a
 * transfer from the managers.
 *
 * @param object_id The object ID we want to request a transfer of.
 * @param manager_count The number of managers the object can be found on.
 * @param manager_vector A vector of the IP addresses of the managers that the
 *        object can be found on.
 * @param context The context for the connection to this client.
 *
 * Initializes a new context for this client and object. Managers are tried in
 * order until we receive the data or we timeout and run out of retries.
 */
void request_transfer(object_id object_id,
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
 * Process a wait request from a client.
 *
 * @param client_conn The connection context for the client that made the
 *        request.
 * @param num_object_requests Number of object requests wait is called on.
 * @param object_requests The array of bject requests wait is called on.
 * @param timeout Wait will time out and return after this number of
 *                milliseconds.
 * @param num_returns Number of object requests that will be satsified
 *                    before wait will retunr, unless it timeouts.
 * @return Void.
 */
void process_wait_request1(client_connection *client_conn,
                           int num_object_requests,
                           object_request object_requests[],
                           uint64_t timeout,
                           int num_ready_objects);

/**
 * Callback to be invoked when object_id entry is changed in the
 * Object Table. We assume that the change means the object is available.
 *
 * @param object_id ID of the object becoming available locally or remotely.
 * @param user_context This is the client connection on which the wait
 *                     has been called.
 * @return Void.
 */
void wait_object_available_callback(object_id object_id, void *user_context);

/**
 * Object is available (sealed) in the local Object Store. This is part of
 * executing wait operation.
 *
 * @param client_conn The client conection.
 * @param user_context This is the client connection on which the wait
 *                     has been called.
 * @return Void.
 */
void wait_process_object_available_local(client_connection *client_conn,
                                         object_id object_id);


#endif /* PLASMA_MANAGER_H */
