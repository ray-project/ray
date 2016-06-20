#include "utils.h"

#if defined(_CPPLIB_VER) && _CPPLIB_VER >= 650
#include <experimental/filesystem>
#else
#include <boost/filesystem.hpp>
#endif

#include "ray/ray.h"

std::string::iterator split_ip_address(std::string& ip_address) {
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

const char* get_cmd_option(char** begin, char** end, const std::string& option) {
  char** it = std::find(begin, end, option);
  if (it != end && ++it != end) {
    return *it;
  }
  return 0;
}

void create_log_dir_or_die(const char* log_file_name) {
#ifdef BOOST_FILESYSTEM_FILESYSTEM_HPP
  namespace filesystem = boost::filesystem;
  typedef boost::system::error_code error_code;
#else
  namespace filesystem = std::experimental::filesystem;
  typedef std::error_code error_code;
#endif
  filesystem::path log_file_path(log_file_name);
  error_code returned_error;
  filesystem::create_directories(log_file_path.parent_path(), returned_error);
  if (returned_error) {
    RAY_CHECK(false, "Failed to create directory for " << log_file_name);
  }
}
