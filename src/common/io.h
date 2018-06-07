#ifndef IO_H
#define IO_H

#include <stdint.h>
#include <stdlib.h>

#include <vector>

struct aeEventLoop;
typedef aeEventLoop event_loop;

enum class CommonMessageType : int32_t {
  /** Disconnect a client. */
  DISCONNECT_CLIENT,
  /** Log a message from a client. */
  LOG_MESSAGE,
  /** Submit a task to the local scheduler. */
  SUBMIT_TASK,
};

/* Helper functions for socket communication. */

/**
 * Binds to an Internet socket at the given port. Removes any existing file at
 * the pathname. Returns a non-blocking file descriptor for the socket, or -1
 * if an error occurred.
 *
 * @note Since the returned file descriptor is non-blocking, it is not
 * recommended to use the Linux read and write calls directly, since these
 * might read or write a partial message. Instead, use the provided
 * write_message and read_message methods.
 *
 * @param port The port to bind to.
 * @param shall_listen Are we also starting to listen on the socket?
 * @return A non-blocking file descriptor for the socket, or -1 if an error
 *         occurs.
 */
int bind_inet_sock(const int port, bool shall_listen);

/**
 * Binds to a Unix domain streaming socket at the given
 * pathname. Removes any existing file at the pathname.
 *
 * @param socket_pathname The pathname for the socket.
 * @param shall_listen Are we also starting to listen on the socket?
 * @return A blocking file descriptor for the socket, or -1 if an error
 *         occurs.
 */
int bind_ipc_sock(const char *socket_pathname, bool shall_listen);

/**
 * Connect to a Unix domain streaming socket at the given
 * pathname.
 *
 * @param socket_pathname The pathname for the socket.
 * @return A file descriptor for the socket, or -1 if an error occurred.
 */
int connect_ipc_sock(const char *socket_pathname);

/**
 * Connect to a Unix domain streaming socket at the given
 * pathname, or fail after some number of retries.
 *
 * @param socket_pathname The pathname for the socket.
 * @param num_retries The number of times to retry the connection
 *        before exiting. If -1 is provided, then this defaults to
 *        num_connect_attempts.
 * @param timeout The number of milliseconds to wait in between
 *        retries. If -1 is provided, then this defaults to
 *        connect_timeout_milliseconds.
 * @return A file descriptor for the socket, or -1 if an error occurred.
 */
int connect_ipc_sock_retry(const char *socket_pathname,
                           int num_retries,
                           int64_t timeout);

/**
 * Connect to an Internet socket at the given address and port.
 *
 * @param ip_addr The IP address to connect to.
 * @param port The port number to connect to.
 *
 * @param socket_pathname The pathname for the socket.
 * @return A file descriptor for the socket, or -1 if an error occurred.
 */
int connect_inet_sock(const char *ip_addr, int port);

/**
 * Connect to an Internet socket at the given address and port, or fail after
 * some number of retries.
 *
 * @param ip_addr The IP address to connect to.
 * @param port The port number to connect to.
 * @param num_retries The number of times to retry the connection
 *        before exiting. If -1 is provided, then this defaults to
 *        num_connect_attempts.
 * @param timeout The number of milliseconds to wait in between
 *        retries. If -1 is provided, then this defaults to
 *        connect_timeout_milliseconds.
 * @return A file descriptor for the socket, or -1 if an error occurred.
 */
int connect_inet_sock_retry(const char *ip_addr,
                            int port,
                            int num_retries,
                            int64_t timeout);

/**
 * Accept a new client connection on the given socket
 * descriptor. Returns a descriptor for the new socket.
 */
int accept_client(int socket_fd);

/* Reading and writing data. */

/**
 * Write a sequence of bytes on a file descriptor. The bytes should then be read
 * by read_message.
 *
 * @param fd The file descriptor to write to. It can be non-blocking.
 * @param version The protocol version.
 * @param type The type of the message to send.
 * @param length The size in bytes of the bytes parameter.
 * @param bytes The address of the message to send.
 * @return int Whether there was an error while writing. 0 corresponds to
 *         success and -1 corresponds to an error (errno will be set).
 */
int write_message(int fd, int64_t type, int64_t length, uint8_t *bytes);

/**
 * Read a sequence of bytes written by write_message from a file descriptor.
 * This allocates space for the message.
 *
 * @note The caller must free the memory.
 *
 * @param fd The file descriptor to read from. It can be non-blocking.
 * @param type The type of the message that is read will be written at this
 *        address. If there was an error while reading, this will be
 *        DISCONNECT_CLIENT.
 * @param length The size in bytes of the message that is read will be written
 *        at this address. This size does not include the bytes used to encode
 *        the type and length. If there was an error while reading, this will
 *        be 0.
 * @param bytes The address at which to write the pointer to the bytes that are
 *        read and allocated by this function. If there was an error while
 *        reading, this will be NULL.
 * @return Void.
 */
void read_message(int fd, int64_t *type, int64_t *length, uint8_t **bytes);

/**
 * Read a message from a file descriptor and remove the file descriptor from the
 * event loop if there is an error. This will actually do two reads. The first
 * read reads sizeof(int64_t) bytes to determine the number of bytes to read in
 * the next read.
 *
 * @param loop: The event loop.
 * @param sock: The file descriptor to read from.
 * @return A byte buffer contining the message or NULL if there was an
 *         error. The buffer needs to be freed by the user.
 */
uint8_t *read_message_async(event_loop *loop, int sock);

/**
 * Read a sequence of bytes written by write_message from a file descriptor.
 * This does not allocate space for the message if the provided buffer is
 * large enough and can therefore often avoid allocations.
 *
 * @param fd The file descriptor to read from. It can be non-blocking.
 * @param type The type of the message that is read will be written at this
 *        address. If there was an error while reading, this will be
 *        DISCONNECT_CLIENT.
 * @param buffer The array the message will be written to. If it is not
 *        large enough to hold the message, it will be enlarged by read_vector.
 * @return Number of bytes of the message that were read. This size does not
 *         include the bytes used to encode the type and length. If there was
 *         an error while reading, this will be 0.
 */
int64_t read_vector(int fd, int64_t *type, std::vector<uint8_t> &buffer);

/**
 * Write a null-terminated string to a file descriptor.
 */
void write_log_message(int fd, const char *message);

/**
 * Reads a null-terminated string from the file descriptor that has been
 * written by write_log_message. Allocates and returns a pointer to the string.
 * NOTE: Caller must free the memory!
 */
char *read_log_message(int fd);

/**
 * Read a sequence of bytes from a file descriptor into a buffer. This will
 * block until one of the following happens: (1) there is an error (2) end of
 * file, or (3) all length bytes have been written.
 *
 * @note The buffer pointed to by cursor must already have length number of
 * bytes allocated before calling this method.
 *
 * @param fd The file descriptor to read from. It can be non-blocking.
 * @param cursor The cursor pointing to the beginning of the buffer.
 * @param length The size of the byte sequence to read.
 * @return int Whether there was an error while reading. 0 corresponds to
 *         success and -1 corresponds to an error (errno will be set).
 */
int read_bytes(int fd, uint8_t *cursor, size_t length);

/**
 * Write a sequence of bytes into a file descriptor. This will block until one
 * of the following happens: (1) there is an error (2) end of file, or (3) all
 * length bytes have been written.
 *
 * @param fd The file descriptor to write to. It can be non-blocking.
 * @param cursor The cursor pointing to the beginning of the bytes to send.
 * @param length The size of the bytes sequence to write.
 * @return int Whether there was an error while writing. 0 corresponds to
 *         success and -1 corresponds to an error (errno will be set).
 */
int write_bytes(int fd, uint8_t *cursor, size_t length);

#endif /* IO_H */
