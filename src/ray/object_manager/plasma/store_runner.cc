#include "ray/object_manager/plasma/store_runner.h"

#ifndef _WIN32
#include <fcntl.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "ray/common/ray_config.h"
#include "ray/object_manager/plasma/plasma_allocator.h"

namespace plasma {
namespace internal {
void SetMallocGranularity(int value);
}

PlasmaStoreRunner::PlasmaStoreRunner(std::string socket_name,
                                     int64_t system_memory,
                                     bool hugepages_enabled,
                                     std::string plasma_directory,
                                     std::string fallback_directory)
    : hugepages_enabled_(hugepages_enabled) {
  // Sanity check.
  if (socket_name.empty()) {
    RAY_LOG(FATAL) << "please specify socket for incoming connections with -s switch";
  }
  socket_name_ = socket_name;
  if (system_memory == -1) {
    RAY_LOG(FATAL) << "please specify the amount of system memory with -m switch";
  }
  RAY_LOG(INFO) << "Allowing the Plasma store to use up to "
                << static_cast<double>(system_memory) / 1000000000 << "GB of memory.";
  if (hugepages_enabled && plasma_directory.empty()) {
    RAY_LOG(FATAL) << "if you want to use hugepages, please specify path to huge pages "
                      "filesystem with -d";
  }
  if (plasma_directory.empty()) {
#ifdef __linux__
    plasma_directory = "/dev/shm";
#else
    plasma_directory = "/tmp";
#endif
  }
  if (fallback_directory.empty()) {
    fallback_directory = "/tmp";
  }
  RAY_LOG(INFO) << "Starting object store with directory " << plasma_directory
                << ", fallback " << fallback_directory << ", and huge page support "
                << (hugepages_enabled ? "enabled" : "disabled");
#ifdef __linux__
  if (!hugepages_enabled) {
    // On Linux, check that the amount of memory available in /dev/shm is large
    // enough to accommodate the request. If it isn't, then fail.
    int shm_fd = open(plasma_directory.c_str(), O_RDONLY);
    struct statvfs shm_vfs_stats;
    fstatvfs(shm_fd, &shm_vfs_stats);
    // The value shm_vfs_stats.f_bsize is the block size, and the value
    // shm_vfs_stats.f_bavail is the number of available blocks.
    int64_t shm_mem_avail = shm_vfs_stats.f_bsize * shm_vfs_stats.f_bavail;
    close(shm_fd);
    // Keep some safety margin for allocator fragmentation.
    shm_mem_avail = 9 * shm_mem_avail / 10;
    if (system_memory > shm_mem_avail) {
      RAY_LOG(WARNING)
          << "System memory request exceeds memory available in " << plasma_directory
          << ". The request is for " << system_memory
          << " bytes, and the amount available is " << shm_mem_avail
          << " bytes. You may be able to free up space by deleting files in "
             "/dev/shm. If you are inside a Docker container, you may need to "
             "pass an argument with the flag '--shm-size' to 'docker run'.";
      system_memory = shm_mem_avail;
    }
  } else {
    internal::SetMallocGranularity(1024 * 1024 * 1024);  // 1 GB
  }
#endif
  system_memory_ = system_memory;
  plasma_directory_ = plasma_directory;
  fallback_directory_ = fallback_directory;
}

void PlasmaStoreRunner::Start(ray::SpillObjectsCallback spill_objects_callback,
                              std::function<void()> object_store_full_callback,
                              ray::AddObjectCallback add_object_callback,
                              ray::DeleteObjectCallback delete_object_callback) {
  SetThreadName("store.io");
  RAY_LOG(DEBUG) << "starting server listening on " << socket_name_;
  {
    absl::MutexLock lock(&store_runner_mutex_);
    allocator_ = std::make_unique<PlasmaAllocator>(
        plasma_directory_, fallback_directory_, hugepages_enabled_, system_memory_);
    store_.reset(new PlasmaStore(main_service_,
                                 *allocator_,
                                 socket_name_,
                                 RayConfig::instance().object_store_full_delay_ms(),
                                 RayConfig::instance().object_spilling_threshold(),
                                 spill_objects_callback,
                                 object_store_full_callback,
                                 add_object_callback,
                                 delete_object_callback));
    store_->Start();
  }
  main_service_.run();
  Shutdown();
}

void PlasmaStoreRunner::Stop() {
  absl::MutexLock lock(&store_runner_mutex_);
  if (store_) {
    store_->Stop();
  }
  main_service_.stop();
}

void PlasmaStoreRunner::Shutdown() {
  absl::MutexLock lock(&store_runner_mutex_);
  if (store_) {
    store_->Stop();
    store_ = nullptr;
  }
}

bool PlasmaStoreRunner::IsPlasmaObjectSpillable(const ObjectID &object_id) {
  return store_->IsObjectSpillable(object_id);
}

int64_t PlasmaStoreRunner::GetConsumedBytes() { return store_->GetConsumedBytes(); }

int64_t PlasmaStoreRunner::GetFallbackAllocated() const {
  absl::MutexLock lock(&store_runner_mutex_);
  return allocator_ ? allocator_->FallbackAllocated() : 0;
}

std::unique_ptr<PlasmaStoreRunner> plasma_store_runner;

}  // namespace plasma
