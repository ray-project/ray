#ifndef PLASMA_MANAGER_H
#define PLASMA_MANAGER_H

#include <poll.h>

#define MAX_CONNECTIONS 2048

enum conn_type {
  // Connection to send commands to the manager.
  CONN_CONTROL,
  // Connection to send data to another manager.
  CONN_WRITE_DATA,
  // Connection to receive data from another manager.
  CONN_READ_DATA
};

typedef struct {
  // Of type conn_type.
  int type;
  // Socket of the plasma store that is accessed for reading or writing data for
  // this connection.
  int store_conn;
  // Buffer this connection is reading from or writing to.
  plasma_buffer buf;
  // Current position in the buffer.
  int64_t cursor;
} conn_state;

typedef struct {
  // ID of this manager
  int64_t manager_id;
  // Name of the socket connecting to local plasma store.
  const char* store_socket_name;
  // Number of connections.
  int num_conn;
  // For the "poll" system call.
  struct pollfd waiting[MAX_CONNECTIONS];
  // Status of connections (both control and data).
  conn_state conn[MAX_CONNECTIONS];
} plasma_manager_state;

#endif
