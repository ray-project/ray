#include "utils.h"

#include "ray/ray.h"

#include <sys/stat.h>
#ifdef _S_IREAD  // Visual C++ runtime?
#include <direct.h>  // _mkdir
#else
namespace {
  int _mkdir(char const* path) {
    return mkdir(path, S_IRWXU | S_IRWXG | S_IRWXO);
  }
}
#endif

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

void create_directories(const char* log_file_name) {
  bool success = _mkdir(log_file_name) != -1 || errno == EEXIST;
  if (!success) {
    // If we couldn't create it directly and it didn't already exist, then try to create it from the root...
    // Note that we keep going until the end even if creating the root fails, because we don't necessarily have access to the root
    bool stop = false;
    size_t i = 0;
    do {
      stop = log_file_name[i] == '\0';
      bool delimiter = stop || log_file_name[i] == '/' || log_file_name[i] == '\\';
      if (!stop) {
        ++i;
      }
      if (delimiter) {
        std::string ancestor(log_file_name, i);
        success = _mkdir(ancestor.c_str()) != -1 || errno == EEXIST;
      }
    } while (!stop);
  }
  RAY_CHECK(success, "Failed to create directory for " << log_file_name);
}

void create_log_dir_or_die(const char* log_file_name) {
  std::string dirname = log_file_name;
  while (!dirname.empty() && dirname.back() != '/' && dirname.back() != '\\') {
    dirname.pop_back();
  }
  return create_directories(dirname.c_str());
}
