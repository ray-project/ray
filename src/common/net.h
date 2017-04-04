#ifndef NET_H
#define NET_H

#include <string>
#include <vector>
#include <sstream>

/* Helper function to parse a string of the form <IP address>:<port> into the
 * given ip_addr and port pointers. The ip_addr buffer must already be
 * allocated. Return 0 upon success and -1 upon failure. */
int parse_ip_addr_port(const char *ip_addr_port, char *ip_addr, int *port);

/* Parse a list of IP addresses of the form [127.0.0.1:10000,127.0.0.1:20000].
 */
bool parse_ip_addrs_ports(std::string ip_addrs_ports,
                          std::vector<std::string>& ip_addrs,
                          std::vector<int>& ports);

#endif /* NET_H */
