#include "net.h"

#include "common.h"

int parse_ip_addr_port(const char *ip_addr_port, char *ip_addr, int *port) {
  char port_str[6];
  int parsed = sscanf(ip_addr_port, "%15[0-9.]:%5[0-9]", ip_addr, port_str);
  if (parsed != 2) {
    return -1;
  }
  *port = atoi(port_str);
  return 0;
}
