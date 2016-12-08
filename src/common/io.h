#ifndef IO_H
#define IO_H

#include <stdbool.h>
#include <stdint.h>

#include "utarray.h"

enum common_message_type {
  /** Disconnect a client. */
  DISCONNECT_CLIENT,
  /** Log a message from a client. */
  LOG_MESSAGE,
  /** Submit a task to the local scheduler. */
  SUBMIT_TASK,
};

/* Helper functions for socket communication. */

int bind_inet_sock(const int port, bool shall_listen);

/**
 * Binds to a Unix domain streaming socket at the given
 * pathname. Removes any existing file at the pathname.
 *
 * @param socket_pathname The pathname for the socket.
 * @param shall_listen Are we also starting to listen on the socket?
 * @param will_be_added_to_event_loop Is this socket used directly,
 *        or is it going to be awaited via adding it to an event loop?
 *        This choice cannot change dynamically and must be made here
 *        on Windows.
 *  NOTE: If you set this parameter, you will have to use a different
 *        function for accepting/listening/connecting on Windows!
 * @return A blocking file descriptor for the socket, or -1 if an error
 *         occurs.
 */
int bind_ipc_sock(const char *socket_pathname,
                  bool shall_listen,
                  bool will_be_added_to_event_loop);
int connect_ipc_sock(const char *socket_pathname);

int accept_client(int socket_fd, bool will_be_added_to_event_loop);

/* Reading and writing data. */

int write_message(int fd, int64_t type, int64_t length, uint8_t *bytes);
void read_message(int fd, int64_t *type, int64_t *length, uint8_t **bytes);
int64_t read_buffer(int fd, int64_t *type, UT_array *buffer);

void write_log_message(int fd, char *message);
void write_formatted_log_message(int fd, const char *format, ...);
char *read_log_message(int fd);
int read_bytes(int fd, uint8_t *cursor, size_t length);
int write_bytes(int fd, uint8_t *cursor, size_t length);

#endif /* IO_H */
