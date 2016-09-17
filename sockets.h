#ifndef SOCKETS_H
#define SOCKETS_H

/* Helper functions for socket communication. */
int bind_ipc_sock(const char* socket_pathname);
int connect_ipc_sock(const char* socket_pathname);
void send_ipc_sock(int socket_fd, char* message);
int accept_client(int socket_fd);
char* recv_ipc_sock(int socket_fd);

#endif
