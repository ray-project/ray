#ifndef PLASMA_MANAGER_H
#define PLASMA_MANAGER_H

#include <poll.h>
#include "utarray.h"

/* The buffer size in bytes. Data will get transfered in multiples of this */
#define BUFSIZE 4096

enum connection_type {
  CONNECTION_REDIS,
  CONNECTION_LISTENER,
  CONNECTION_DATA
};

enum data_connection_type {
  /* Connection to send commands and metadata to the manager. */
  DATA_CONNECTION_HEADER,
  /* Connection to send data to another manager. */
  DATA_CONNECTION_WRITE,
  /* Connection to receive data from another manager. */
  DATA_CONNECTION_READ
};

typedef struct {
  /* Of type data_connection_type. */
  int type;
  /* Local socket of the plasma store that is accessed for reading or writing
   * data for this connection. */
  int store_conn;
  /* Buffer this connection is reading from or writing to. */
  plasma_buffer buf;
  /* Current position in the buffer. */
  int64_t cursor;
} data_connection;

#endif
