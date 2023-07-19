#include "ray/object_manager/plugin_manager.h"
#include "nlohmann/json.hpp"

using json = nlohmann::json;

namespace ray {

PluginManager &PluginManager::GetInstance() {
  static PluginManager instance;
  return instance;
}

void PluginManager::LoadObjectStorePlugin(const std::string plugin_name, const std::string plugin_path, const std::string plugin_params) {
  using ObjectStoreRunnerCreator = std::unique_ptr<plasma::ObjectStoreRunnerInterface> (*)();
  using ObjectStoreClientCreator = std::shared_ptr<plasma::ObjectStoreClientInterface> (*)();

  RAY_LOG(INFO) << " yiweizh: Calling LoadOBjectStorePlugin with name " << plugin_name;
  void *handle = dlopen(plugin_path.c_str(), RTLD_NOW);
  if (!handle) {
    std::cerr << "Failed to load shared library: " << dlerror() << std::endl;
    return;
  }

  //ObjectStoreClientCreator client_creator = reinterpret_cast<ObjectStoreClientCreator>(dlsym(handle, "CreateNewStoreClient"));
  client_creators_[plugin_name] = reinterpret_cast<ObjectStoreClientCreator>(dlsym(handle, "CreateNewStoreClient"));
  if (!client_creators_[plugin_name]) {
    std::cerr << "Failed to get CreateNewStoreClient function: " << dlerror()
              << std::endl;
    dlclose(handle);
    return;
  }

  ObjectStoreRunnerCreator runner_creator = reinterpret_cast<ObjectStoreRunnerCreator>(dlsym(handle, "CreateNewStoreRunner"));
  if (!runner_creator) {
    std::cerr << "Failed to get CreateNewStoreRunner function: " << dlerror()
              << std::endl;
    dlclose(handle);
    return;
  }

  //client_creators_[plugin_name] = client_creator; 
  //std::shared_ptr<plasma::ObjectStoreClientInterface> client = client_creator();
  std::shared_ptr<plasma::ObjectStoreClientInterface> client = client_creators_[plugin_name]();
  std::unique_ptr<plasma::ObjectStoreRunnerInterface> runner = std::move(runner_creator());

  object_stores_[plugin_name] = PluginManagerObjectStore{
    client,
    std::move(runner)
  };
  dlclose(handle);
  return;
}

std::shared_ptr<plasma::ObjectStoreClientInterface> PluginManager::CreateCurrentClientInstance(){
    RAY_LOG(INFO) << "plugin_manager CreateCurrentClientInterface " << current_object_store_name_;
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
                                    const std::string plugin_path,
                                    const std::string plugin_params) {
    RAY_LOG(INFO) << "Entering PluginManager::SetDefaultObjectStore " << plugin_name ;
    json params_map = json::parse(plugin_params);
    // Parse the plugin params
    std::string store_socket_name_ = params_map["store_socket_name"];
    RAY_LOG(INFO) << store_socket_name_;
    std::string plasma_directory_ = params_map["plasma_directory"];
    RAY_LOG(INFO) << "plasma_directory_ pages " << plasma_directory_;
    std::string fallback_directory_ = params_map["temp_dir"];
    RAY_LOG(INFO) << "fallback_directory_ " << fallback_directory_;

    int64_t object_store_memory_ = params_map["object_store_memory"]; 
    RAY_LOG(INFO) << "object_store_memory_ " << object_store_memory_;
    bool huge_pages_ = false;
    if (!params_map["huge_pages"].is_null() && params_map["huge_pages"] == true){
        huge_pages_ = true;
    }
    RAY_LOG(INFO) << "huge_pages " << huge_pages_ ;
    


    

    // LoadPlugin
    if (plugin_name != "default"){
        current_object_store_name_ = plugin_name;
        LoadObjectStorePlugin(plugin_name, plugin_path, plugin_params);
    } else {
        current_object_store_name_ = "default";

        // object_stores_["default"] = PluginManagerObjectStore{
        //     std::make_shared<plasma::PlasmaClient>(),
        //     std::make_unique<plasma::PlasmaStoreRunner>(store_socket_name,
        //                                                 object_store_memory,
        //                                                 huge_pages,
        //                                                 plasma_directory,
        //                                                 fallback_directory)
        // };
        object_stores_["default"] = PluginManagerObjectStore{
            std::make_shared<plasma::PlasmaClient>(),
            std::make_unique<plasma::PlasmaStoreRunner>(store_socket_name_,
                                                        object_store_memory_,
                                                        huge_pages_,
                                                        plasma_directory_,
                                                        fallback_directory_)
        };
    }
}

std::string PluginManager::GetCurrentObjectStoreName() {
    return current_object_store_name_;
}


}  // namespace ray

