#include "ray/object_manager/plugin_manager.h"


namespace ray {

void PluginManager::LoadObjectStorePlugin(const std::string plugin_name, const std::string plugin_path, const std::string plugin_params) {
  using ObjectStoreRunnerCreator = std::unique_ptr<plasma::ObjectStoreRunnerInterface> (*)();
  using ObjectStoreClientCreator = std::shared_ptr<plasma::ObjectStoreClientInterface> (*)();

  RAY_LOG(INFO) << " yiweizh: Calling LoadOBjectStorePlugin with name " << plugin_name;
  void *handle = dlopen(plugin_path.c_str(), RTLD_NOW);
  if (!handle) {
    std::cerr << "Failed to load shared library: " << dlerror() << std::endl;
    return;
  }

  ObjectStoreClientCreator client_creator = reinterpret_cast<ObjectStoreClientCreator>(dlsym(handle, "CreateNewStoreClient"));
  //std::shared_ptr<plasma::ObjectStoreClientInterface> client_creator =
      //reinterpret_cast<std::shared_ptr<plasma::ObjectStoreClientInterface>>(dlsym(handle, "CreateNewStoreClient"));
  if (!client_creator) {
    std::cerr << "Failed to get CreateNewStoreClient function: " << dlerror()
              << std::endl;
    dlclose(handle);
    return;
  }

  ObjectStoreRunnerCreator runner_creator = reinterpret_cast<ObjectStoreRunnerCreator>(dlsym(handle, "CreateNewStoreRunner"));

  //std::unique_ptr<plasma::ObjectStoreRunnerInterface> runner_creator =
      //reinterpret_cast<std::unique_ptr<plasma::ObjectStoreRunnerInterface>>(dlsym(handle, "CreateNewStoreRunner"));
  if (!runner_creator) {
    std::cerr << "Failed to get CreateNewStoreRunner function: " << dlerror()
              << std::endl;
    dlclose(handle);
    return;
  }
  client_creators_[plugin_name] = client_creator; 
  std::shared_ptr<plasma::ObjectStoreClientInterface> client = client_creator();
  std::unique_ptr<plasma::ObjectStoreRunnerInterface> runner = std::move(runner_creator());
//   PluginManagerObjectStore plugin_object_store;
//   plugin_object_store.client_ = client_creator;
//   plugin_object_store.runner_ = std::move(runner_creator);
//   object_stores_[plugin_name] = plugin_object_store;
  object_stores_[plugin_name] = PluginManagerObjectStore{
    client,
    std::move(runner)
  };
  dlclose(handle);
  return;
}

std::shared_ptr<plasma::ObjectStoreClientInterface> PluginManager::CreateCurrentClientInstance(){
    if (current_object_store_name_ == "default"){
        return std::make_shared<plasma::PlasmaClient>();
    } else {
        return client_creators_[current_object_store_name_]();
    }
}

std::shared_ptr<plasma::ObjectStoreClientInterface> PluginManager::CreateObjectStoreClientInstance(
    const std::string &name) {
  return object_stores_[name].client_; 
}

std::unique_ptr<plasma::ObjectStoreRunnerInterface> PluginManager::CreateObjectStoreRunnerInstance(
    const std::string &name) {
  return std::move(object_stores_[name].runner_);
}

void PluginManager::SetObjectStores(const std::string plugin_name, 
                                    const std::string store_socket_name,
                                    const int64_t object_store_memory,
                                    const bool huge_pages,
                                    const std::string plasma_directory,
                                    const std::string fallback_directory,
                                    const std::string plugin_path,
                                    const std::string plugin_params) {
    RAY_LOG(INFO) << "Entering PluginManager::SetDefaultObjectStore " << plugin_name ;
    // i
    object_stores_["default"] = PluginManagerObjectStore{
        std::make_shared<plasma::PlasmaClient>(),
        std::make_unique<plasma::PlasmaStoreRunner>(store_socket_name,
                                                    object_store_memory,
                                                    huge_pages,
                                                    plasma_directory,
                                                    fallback_directory)
    };
    // LoadPlugin
    if (plugin_name != "default"){
        LoadObjectStorePlugin(plugin_name, plugin_path, plugin_params);
        current_object_store_name_ = plugin_name;
    }
    // object_stores_[config.plugin_name] = Plugin
}


}  // namespace ray

