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

extern std::unique_ptr<PlasmaStoreRunner> plasma_store_runner;

}  // namespace plasma

#endif  // PLASMA_STORE_RUNNER_H
