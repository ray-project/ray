#include "ray/object_manager/plugin_manager.h"


void PluginManager::LoadObjectStorePlugin(const std::string plugin_name) {
  RAY_LOG(INFO) << " yiweizh: Calling LoadOBjectStorePlugin with name " << plugin_name;
  void *handle = dlopen(plugin_name.c_str(), RTLD_NOW);
  if (!handle) {
      std::cerr << "Failed to load shared library: " << dlerror() << std::endl;
      return;
  }
  
  std::string (*PluginStart)() = reinterpret_cast<std::string (*)()>(dlsym(handle, "PluginStart"));
  if (dlerror() != nullptr) {
      std::cerr << dlerror() << std::endl;
      dlclose(handle);
      return;
  }

  // RAY_LOG(INFO) << PluginStart();
  std::cout << "Plugin output: " << PluginStart() << std::endl;
  dlclose(handle);

  return;
}

