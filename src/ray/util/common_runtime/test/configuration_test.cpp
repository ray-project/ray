#include <gtest/gtest.h>

#include "common_runtime.h"

TEST(ConfigurationTest, ReadConfiguration) {
  std::map<std::string, std::map<std::string, std::string>> config;
  CrReadConfiguration("test_config.ini", "", &config);

  for (const auto &section_pair : config) {
    std::cout << "[" << section_pair.first << "]"
              << "\n";
    for (const auto &kv_pair : section_pair.second) {
      std::cout << kv_pair.first << " = " << kv_pair.second << "\n";
    }
  }
}