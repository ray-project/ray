#ifndef ORCHESTRA_UTILS_H
#define ORCHESTRA_UTILS_H

inline std::string::iterator split_ip_address(std::string& ip_address) {
  if (ip_address[0] == '[') { // IPv6
    auto split_end = std::find(ip_address.begin() + 1, ip_address.end(), ']');
    if(split_end != ip_address.end()) {
      split_end++;
    }
    if(split_end != ip_address.end() && *split_end == ':') {
      return split_end;
    }
    ORCH_LOG(ORCH_FATAL, "ip address should contain a port number");
  } else { // IPv4
    auto split_point = std::find(ip_address.rbegin(), ip_address.rend(), ':').base();
    if (split_point == ip_address.begin()) {
      ORCH_LOG(ORCH_FATAL, "ip address should contain a port number");
    } else {
      return split_point;
    }
  }
}

#endif
