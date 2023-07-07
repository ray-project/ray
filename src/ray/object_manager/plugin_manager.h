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
//   using ObjectStoreRunnerCreator = std::unique_ptr<ObjectStoreRunnerInterface> (*)();
  using ObjectStoreClientCreator = std::shared_ptr<plasma::ObjectStoreClientInterface> (*)();


  std::unique_ptr<plasma::ObjectStoreRunnerInterface> CreateObjectStoreRunnerInstance(const std::string& name);
  std::shared_ptr<plasma::ObjectStoreClientInterface> CreateObjectStoreClientInstance(const std::string& name);
  std::shared_ptr<plasma::ObjectStoreClientInterface> CreateCurrentClientInstance();
  void LoadObjectStorePlugin(const std::string plugin_name, const std::string plugin_path, const std::string plugin_params);
  void SetObjectStores(const std::string plugin_name, 
                       const std::string store_socket_name,
                       const int64_t object_store_memory,
                       const bool huge_pages,
                       const std::string plasma_directory,
                       const std::string fallback_directory,
                       const std::string plugin_path,
                       const std::string plugin_params);

  static PluginManager& GetInstance() {
    static PluginManager instance;
    //instance.SetObjectStores(config);
    return instance;
  }
  PluginManager(){}
  ~PluginManager(){}

 private:
  PluginManager(const PluginManager&) = delete;
  PluginManager& operator=(const PluginManager&) = delete;

  std::map<std::string, PluginManagerObjectStore> object_stores_;
  std::map<std::string, ObjectStoreClientCreator> client_creators_;
  std::string current_object_store_name_ = "default";
};

} // namespace ray
