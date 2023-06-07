#include <dlfcn.h>
#include <iostream>
#include "ray/util/logging.h"


class PluginManager {
 public:
  static PluginManager& GetInstance() {
    static PluginManager instance;
    return instance;
  }
  void LoadObjectStorePlugin(const std::string plugin_name);
  PluginManager() = default;
  ~PluginManager() = default;

 private:
  PluginManager(const PluginManager&) = delete;
  PluginManager& operator=(const PluginManager&) = delete;

};
