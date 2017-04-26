#include "net.h"

#include <arpa/inet.h>

#include <sstream>

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

/* Return true if the ip address is valid. */
bool valid_ip_address(const std::string &ip_address) {
  struct sockaddr_in sa;
  int result = inet_pton(AF_INET, ip_address.c_str(), &sa.sin_addr);
  return result == 1;
}

bool parse_ip_addrs_ports(std::string ip_addrs_ports,
                          std::vector<std::string>& ip_addrs,
                          std::vector<int>& ports) {
  std::stringstream stream(ip_addrs_ports);
  char ch;
  stream >> ch; // consume the initial '['.
  if (ch != '[') {
    return false;
  }
  do {
    std::string token;
    std::getline(stream, token, ':');
    if (!valid_ip_address(token.c_str())) {
      return false;
    }
    ip_addrs.push_back(token);
    int port;
    if (!(stream >> port)) {
      return false;
    }
    ports.push_back(port);
    stream >> ch;
  } while (ch == ',');

  if (ch != ']') {
    return false;
  }

  return true;
}
