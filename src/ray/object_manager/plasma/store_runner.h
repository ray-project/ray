#pragma once

#include <boost/asio.hpp>
#include <memory>

#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/object_manager/plasma/plasma_allocator.h"
#include "ray/object_manager/plasma/store.h"

namespace plasma {

class PlasmaStoreRunner {
 public:
  PlasmaStoreRunner(std::string socket_name,
                    int64_t system_memory,
                    bool hugepages_enabled,
                    std::string plasma_directory,
                    std::string fallback_directory);
  void Start(ray::SpillObjectsCallback spill_objects_callback,
             std::function<void()> object_store_full_callback,
             ray::AddObjectCallback add_object_callback,
             ray::DeleteObjectCallback delete_object_callback);
  void Stop();

  bool IsPlasmaObjectSpillable(const ObjectID &object_id);

  int64_t GetConsumedBytes();
  int64_t GetFallbackAllocated() const;

  void GetAvailableMemoryAsync(std::function<void(size_t)> callback) const {
    main_service_.post([this, callback]() { store_->GetAvailableMemory(callback); },
                       "PlasmaStoreRunner.GetAvailableMemory");
  }

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
  std::unique_ptr<PlasmaStore> store_;
};

// We use a global variable for Plasma Store instance here because:
// 1) There is only one plasma store thread in Raylet.
// 2) The thirdparty dlmalloc library cannot be contained in a local variable,
//    so even we use a local variable for plasma store, it does not provide
//    better isolation.
extern std::unique_ptr<PlasmaStoreRunner> plasma_store_runner;

}  // namespace plasma
