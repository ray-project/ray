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

bool parse_ip_addrs_ports(std::string ip_addrs_ports,
                          std::vector<std::string>& ip_addrs,
                          std::vector<int>& ports) {
  if (ip_addrs_ports.front() != '[' || ip_addrs_ports.back() != ']') {
    return false;
  }

  std::stringstream stream(ip_addrs_ports);
  char ch;
  stream >> ch; // consume the initial '['.
  do {
    std::string token;
    std::getline(stream, token, ':');
    // TODO(pcm): Validate that token is a valid IP address.
    ip_addrs.push_back(token);
    int port;
    stream >> port;
    // TODO(pcm): Validate that port is a valid port.
    ports.push_back(port);
    stream >> ch;
  } while (ch == ',');

  return true;
}
