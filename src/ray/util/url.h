#ifndef RAY_UTIL_URL_H
#define RAY_UTIL_URL_H

#include <boost/asio/ip/tcp.hpp>

// Parses the endpoint (host + port number) of a URL.
boost::asio::ip::tcp::endpoint parse_ip_tcp_endpoint(const std::string &endpoint,
                                                     int default_port = 0);

#endif
