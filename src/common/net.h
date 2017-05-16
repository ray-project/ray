#ifndef NET_H
#define NET_H

/* Helper function to parse a string of the form <IP address>:<port> into the
 * given ip_addr and port pointers. The ip_addr buffer must already be
 * allocated. Return 0 upon success and -1 upon failure. */
int parse_ip_addr_port(const char *ip_addr_port, char *ip_addr, int *port);

#endif /* NET_H */
