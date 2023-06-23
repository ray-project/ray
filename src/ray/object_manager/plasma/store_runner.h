#pragma once

#include <boost/asio.hpp>
#include <memory>

#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/file_system_monitor.h"
#include "ray/object_manager/plasma/plasma_allocator.h"
#include "ray/object_manager/plasma/store.h"
#include "ray/object_manager/plasma/object_store_runner_interface.h"

namespace plasma {

class PlasmaStoreRunner : public ObjectStoreRunnerInterface {
 public:
  PlasmaStoreRunner(std::string socket_name,
                    int64_t system_memory,
                    bool hugepages_enabled,
                    std::string plasma_directory,
                    std::string fallback_directory);
  void Start(const std::map<std::string, std::string>& params,
                     ray::SpillObjectsCallback spill_objects_callback,
                     std::function<void()> object_store_full_callback,
                     ray::AddObjectCallback add_object_callback,
                     ray::DeleteObjectCallback delete_object_callback) override;
  void Stop() override;
  
  bool IsObjectSpillable(const ObjectID &object_id) override;

  int64_t GetConsumedBytes() override;

  int64_t GetFallbackAllocated() const override;

  void GetAvailableMemoryAsync(std::function<void(size_t)> callback) const override {
    main_service_.post([this, callback]() { store_->GetAvailableMemory(callback); },
                        "PlasmaStoreRunner.GetAvailableMemory");
  }

  int64_t GetTotalMemorySize() const override { return system_memory_; };
  int64_t GetMaxMemorySize() const override { return system_memory_; };

 private:
  void Shutdown();
  mutable absl::Mutex store_runner_mutex_;
  std::string socket_name_;
  int64_t system_memory_;
  bool hugepages_enabled_;
  std::string plasma_directory_;
  std::string fallback_directory_;
  mutable instrumented_io_context main_service_;
  std::unique_ptr<PlasmaAllocator> allocator_;
  std::unique_ptr<ray::FileSystemMonitor> fs_monitor_;
  std::unique_ptr<PlasmaStore> store_;
};

// We use a global variable for Plasma Store instance here because:
// 1) There is only one plasma store thread in Raylet.
// 2) The thirdparty dlmalloc library cannot be contained in a local variable,
//    so even we use a local variable for plasma store, it does not provide
//    better isolation.
//extern std::unique_ptr<PlasmaStoreRunner> plasma_store_runner;
extern std::unique_ptr<ObjectStoreRunnerInterface> plasma_store_runner;

} // namespace plasma
