#ifndef PLASMA_STORE_RUNNER_H
#define PLASMA_STORE_RUNNER_H

#include <memory>

#include "ray/object_manager/plasma/store.h"

namespace plasma {

class PlasmaStoreRunner {
 public:
  PlasmaStoreRunner(std::string socket_name, int64_t system_memory,
                    bool hugepages_enabled, std::string plasma_directory,
                    const std::string external_store_endpoint);
  void Start();
  void Stop();
  void Shutdown();

 private:
  std::string socket_name_;
  int64_t system_memory_;
  bool hugepages_enabled_;
  std::string plasma_directory_;
  std::string external_store_endpoint_;
  std::unique_ptr<EventLoop> loop_;
  std::unique_ptr<PlasmaStore> store_;
};

// We use a global variable for Plasma Store instance here because:
// 1) There is only one plasma store thread in Raylet or the Plasma Store process.
// 2) The thirdparty dlmalloc library cannot be contained in a local variable,
//    so even we use a local variable for plasma store, it does not provide
//    better isolation.
extern std::unique_ptr<PlasmaStoreRunner> plasma_store_runner;

}  // namespace plasma

#endif  // PLASMA_STORE_RUNNER_H
