#pragma once

#include <dlfcn.h>
#include <iostream>
#include <map>
#include <memory>
#include "ray/util/logging.h"

#include "ray/object_manager/plasma/object_store_client_interface.h"
#include "ray/object_manager/plasma/object_store_runner_interface.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/object_manager/plasma/store_runner.h"


namespace ray {
    
struct PluginManagerObjectStore{
  std::shared_ptr<plasma::ObjectStoreClientInterface> client_;
  std::unique_ptr<plasma::ObjectStoreRunnerInterface> runner_;
};

class PluginManager {
  public:
  //using ObjectStoreRunnerCreator = std::unique_ptr<ObjectStoreRunnerInterface> (*)();
  using ObjectStoreClientCreator = std::shared_ptr<plasma::ObjectStoreClientInterface> (*)();


  std::unique_ptr<plasma::ObjectStoreRunnerInterface> CreateObjectStoreRunnerInstance(const std::string& name);
  std::shared_ptr<plasma::ObjectStoreClientInterface> CreateObjectStoreClientInstance(const std::string& name);
  std::shared_ptr<plasma::ObjectStoreClientInterface> CreateCurrentClientInstance();
  std::string GetCurrentObjectStoreName();

  // For where it uses only client (node manager, plasma store provider, etc.)
  void SetObjectStoreClients(const std::string plugin_name,
                             const std::string plugin_path,
                             const std::string plugin_params);
  void LoadObjectStoreClientPlugin(const std::string plugin_name,
                                   const std::string plugin_path,
                                   const std::string plugin_params);
  // For where it uses both client and runner
  void SetObjectStores(const std::string plugin_name, 
                       const std::string plugin_path,
                       const std::string plugin_params);
  void LoadObjectStorePlugin(const std::string plugin_name, 
                             const std::string plugin_path, 
                             const std::string plugin_params);


  static PluginManager &GetInstance();

 private:
  PluginManager(const PluginManager&) = delete;
  PluginManager(PluginManager &&) = delete;
  PluginManager& operator=(const PluginManager&) = delete;
  PluginManager& operator=(PluginManager &&) = delete;
  PluginManager(): current_object_store_name_("default"){}
  ~PluginManager(){}

  std::map<std::string, PluginManagerObjectStore> object_stores_;
  std::map<std::string, ObjectStoreClientCreator> client_creators_;
  std::string current_object_store_name_;
};

} // namespace ray
