#ifndef POLL_H
#define POLL_H

typedef unsigned long int nfds_t;
int poll(struct pollfd fds[], nfds_t nfds, int timeout);

#endif /* POLL_H */
