#ifndef RAY_UTILS_H
#define RAY_UTILS_H

inline std::string::iterator split_ip_address(std::string& ip_address) {
  if (ip_address[0] == '[') { // IPv6
    auto split_end = std::find(ip_address.begin() + 1, ip_address.end(), ']');
    if(split_end != ip_address.end()) {
      split_end++;
    }
    if(split_end != ip_address.end() && *split_end == ':') {
      return split_end;
    }
    RAY_CHECK(false, "ip address should contain a port number");
  } else { // IPv4
    auto split_point = std::find(ip_address.rbegin(), ip_address.rend(), ':').base();
    RAY_CHECK_NEQ(split_point, ip_address.begin(), "ip address should contain a port number");
    return split_point;
  }
}

#endif
