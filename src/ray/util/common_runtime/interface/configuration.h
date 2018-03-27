#ifndef RAY_UTIL_COMMON_RUNTIME_INTERFACE_CONFIGURATION_H
#define RAY_UTIL_COMMON_RUNTIME_INTERFACE_CONFIGURATION_H

#include <map>
#include <string>

/// read all configuration from a given file with certain arguments
///
/// \param file given config file with sections and key=[value] pairs in sections
/// \param args arguments to adapt the config file, in the format of [section.key[=[value]];]*
/// \param config[out] output configuration
/// \return success or not
bool CrReadConfiguration(
    const std::string &file, const std::string &args,
    std::map<std::string, std::map<std::string, std::string>> *config);

#endif  // RAY_UTIL_COMMON_RUNTIME_INTERFACE_CONFIGURATION_H