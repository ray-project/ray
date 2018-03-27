#include <dsn/utility/configuration.h>
#include <string>
#include <vector>

#include "configuration.h"

bool CrReadConfiguration(
    const std::string &file, const std::string &args,
    std::map<std::string, std::map<std::string, std::string>> *config) {
  dsn::configuration conf;
  conf.load(file.c_str(), args.c_str());

  std::vector<const char *> sections;
  conf.get_all_section_ptrs(sections);

  for (const char *section : sections) {
    auto &section_map =
        config->emplace(section, std::map<std::string, std::string>()).first->second;

    std::vector<const char *> keys;
    conf.get_all_keys(section, keys);
    for (const char *key : keys) {
      section_map.emplace(key, conf.get_value(section, key, std::string(), ""));
    }
  }
}