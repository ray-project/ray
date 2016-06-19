#ifndef RAY_UTILS_H
#define RAY_UTILS_H

#include <string>

std::string::iterator split_ip_address(std::string& ip_address);

const char* get_cmd_option(char** begin, char** end, const std::string& option);

void create_log_dir_or_die(const char* log_file_name);

#endif
