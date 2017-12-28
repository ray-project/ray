#ifndef PLASMA_MANAGER_H
#define PLASMA_MANAGER_H

#include <poll.h>

#ifndef RAY_NUM_RETRIES
#define NUM_RETRIES 5
#else
#define NUM_RETRIES RAY_NUM_RETRIES
#endif

typedef struct PlasmaManagerState PlasmaManagerState;
typedef struct ClientConnection ClientConnection;

/**
 * Initializes the plasma manager state. This connects the manager to the local
 * plasma store, starts the manager listening for client connections, and
 * connects the manager to a database if there is one. The returned manager
 * state should be freed using the provided PlasmaManagerState_destroy
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
PlasmaManagerState *PlasmaManagerState_init(const char *store_socket_name,
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
void PlasmaManagerState_free(PlasmaManagerState *state);

/**
 * Process a request from another object store manager to transfer an object.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param object_id The object_id of the object we will be sending.
 * @param addr The IP address of the plasma manager to send the object to.
 * @param port The port of the plasma manager we are sending the object to.
 * @param conn The ClientConnection to the other plasma manager.
 * @return Void.
 *
 * This establishes a connection to the remote manager if one doesn't already
 * exist, and queues up the request to transfer the data to the other object
 * manager.
 */
void process_transfer(event_loop *loop,
                      ray::ObjectID object_id,
                      uint8_t addr[4],
                      int port,
                      ClientConnection *conn);

/**
 * Process a request from another object store manager to receive data.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param client_sock The connection to the other plasma manager.
 * @param object_id The object_id of the object we will be reading.
 * @param data_size Size of the object.
 * @param metadata_size Size of the metadata.
 * @param conn The ClientConnection to the other plasma manager.
 * @return Void.
 *
 * Initializes the object we are going to write to in the local plasma store
 * and then switches the data socket to read the raw object bytes instead of
 * plasma requests.
 */
void process_data(event_loop *loop,
                  int client_sock,
                  ray::ObjectID object_id,
                  int64_t data_size,
                  int64_t metadata_size,
                  ClientConnection *conn);

/**
 * Read the next chunk of the object in transit from the plasma manager
 * connected to the given socket. Once all data for this object has been read,
 * the socket switches to listening for the next plasma request.
 *
 * @param loop This is the event loop of the plasma manager.
 * @param data_sock The connection to the other plasma manager.
 * @param context The ClientConnection to the other plasma manager.
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
 * @param context The ClientConnection to the other plasma manager, contains a
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
ClientConnection *ClientConnection_listen(event_loop *loop,
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
typedef struct PlasmaRequestBuffer {
  int type;
  ray::ObjectID object_id;
  uint8_t *data;
  int64_t data_size;
  uint8_t *metadata;
  int64_t metadata_size;
} PlasmaRequestBuffer;

/**
 * Call the request_transfer method, which well attempt to get an object from
 * a remote Plasma manager. If it is unable to get it from another Plasma
 * manager, it will cycle through a list of Plasma managers that have the
 * object. This method is only called from the tests.
 *
 * @param object_id The object ID of the object to transfer.
 * @param manager_vector The Plasma managers that have the object.
 * @param context The plasma manager state.
 * @return Void.
 */
void call_request_transfer(ray::ObjectID object_id,
                           const std::vector<std::string> &manager_vector,
                           void *context);

/*
 * This runs periodically (every manager_timeout_milliseconds milliseconds) and
 * reissues transfer requests for all outstanding fetch requests. This is only
 * exposed so that it can be called from the tests.
 */
int fetch_timeout_handler(event_loop *loop, timer_id id, void *context);

/**
 * Get a connection to the remote manager at the specified address. Creates a
 * new connection to this manager if one doesn't already exist.
 *
 * @param state Our plasma manager state.
 * @param ip_addr The IP address of the remote manager we want to connect to.
 * @param port The port that the remote manager is listening on.
 * @return A pointer to the connection to the remote manager.
 */
ClientConnection *get_manager_connection(PlasmaManagerState *state,
                                         const char *ip_addr,
                                         int port);

/**
 * Reads an object chunk sent by the given client into a buffer. This is the
 * complement to write_object_chunk.
 *
 * @param conn The connection to the client who's sending the data. The
 *        connection's cursor will be reset if this is the last read for the
 *        current object.
 * @param buf The buffer to write the data into.
 * @return The errno set, if the read wasn't successful.
 */
int read_object_chunk(ClientConnection *conn, PlasmaRequestBuffer *buf);

/**
 * Writes an object chunk from a buffer to the given client. This is the
 * complement to read_object_chunk.
 *
 * @param conn The connection to the client who's receiving the data. The
 *        connection's cursor will be reset if this is the last write for the
 *        current object.
 * @param buf The buffer to read data from.
 * @return The errno set, if the write wasn't successful.
 */
int write_object_chunk(ClientConnection *conn, PlasmaRequestBuffer *buf);

/**
 * Start a new request on this connection.
 *
 * @param conn The connection on which the request is being sent.
 * @return Void.
 */
void ClientConnection_start_request(ClientConnection *client_conn);

/**
 * Finish the current request on this connection.
 *
 * @param conn The connection on which the request is being sent.
 * @return Void.
 */
void ClientConnection_finish_request(ClientConnection *client_conn);

/**
 * Check whether the current request on this connection is finished.
 *
 * @param conn The connection on which the request is being sent.
 * @return Whether the request has finished.
 */
bool ClientConnection_request_finished(ClientConnection *client_conn);

/**
 * Get the event loop of the given plasma manager state.
 *
 * @param state The state of the plasma manager whose loop we want.
 * @return A pointer to the manager's event loop.
 */
event_loop *get_event_loop(PlasmaManagerState *state);

/**
 * Get the file descriptor for the given client's socket. This is the socket
 * that the client sends or reads data through.
 *
 * @param conn The connection to the client who's sending or reading data.
 * @return A file descriptor for the socket.
 */
int get_client_sock(ClientConnection *conn);

/**
 * Return whether or not the object is local.
 *
 * @param state The state of the plasma manager.
 * @param object_id The ID of the object we want to find.
 * @return A bool that is true if the requested object is local and false
 *         otherwise.
 */
bool is_object_local(PlasmaManagerState *state, ray::ObjectID object_id);

#endif /* PLASMA_MANAGER_H */
