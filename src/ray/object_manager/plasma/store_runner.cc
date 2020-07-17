#include "ray/object_manager/plasma/store_runner.h"

#include <fcntl.h>
#include <stdio.h>
#ifndef _WIN32
#include <sys/statvfs.h>
#endif
#include <sys/types.h>
#include <unistd.h>

#include "ray/object_manager/plasma/io.h"
#include "ray/object_manager/plasma/plasma_allocator.h"

namespace plasma {

void SetMallocGranularity(int value);

PlasmaStoreRunner::PlasmaStoreRunner(std::string socket_name, int64_t system_memory,
                     bool hugepages_enabled, std::string plasma_directory,
                     const std::string external_store_endpoint):
    hugepages_enabled_(hugepages_enabled), external_store_endpoint_(external_store_endpoint) {
  // Sanity check.
  if (socket_name.empty()) {
    RAY_LOG(FATAL) << "please specify socket for incoming connections with -s switch";
  }
  socket_name_ = socket_name;
  if (system_memory == -1) {
    RAY_LOG(FATAL) << "please specify the amount of system memory with -m switch";
  }
  // Set system memory capacity
  PlasmaAllocator::SetFootprintLimit(static_cast<size_t>(system_memory));
  RAY_LOG(INFO) << "Allowing the Plasma store to use up to "
                  << static_cast<double>(system_memory) / 1000000000
                  << "GB of memory.";
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
  RAY_LOG(INFO) << "Starting object store with directory " << plasma_directory
                  << " and huge page support "
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
    SetMallocGranularity(1024 * 1024 * 1024);  // 1 GB
  }
#endif
  system_memory_ = system_memory;
  plasma_directory_ = plasma_directory;
}

void PlasmaStoreRunner::Start() {
#ifdef _WINSOCKAPI_
  WSADATA wsadata;
  WSAStartup(MAKEWORD(2, 2), &wsadata);
#endif
   // Get external store
  std::shared_ptr<plasma::ExternalStore> external_store{nullptr};
  if (!external_store_endpoint_.empty()) {
    std::string name;
    RAY_CHECK_OK(
        plasma::ExternalStores::ExtractStoreName(external_store_endpoint_, &name));
    external_store = plasma::ExternalStores::GetStore(name);
    if (external_store == nullptr) {
      RAY_LOG(FATAL) << "No such external store \"" << name << "\"";
    }
    RAY_LOG(DEBUG) << "connecting to external store...";
    RAY_CHECK_OK(external_store->Connect(external_store_endpoint_));
  }
  RAY_LOG(DEBUG) << "starting server listening on " << socket_name_;

  // Create the event loop.
  loop_.reset(new EventLoop);
  store_.reset(new PlasmaStore(loop_.get(), plasma_directory_, hugepages_enabled_,
                               socket_name_, external_store));
  plasma_config = store_->GetPlasmaStoreInfo();

  // We are using a single memory-mapped file by mallocing and freeing a single
  // large amount of space up front. According to the documentation,
  // dlmalloc might need up to 128*sizeof(size_t) bytes for internal
  // bookkeeping.
  void* pointer = PlasmaAllocator::Memalign(
      kBlockSize, PlasmaAllocator::GetFootprintLimit() - 256 * sizeof(size_t));
  RAY_CHECK(pointer != nullptr);
  // This will unmap the file, but the next one created will be as large
  // as this one (this is an implementation detail of dlmalloc).
  PlasmaAllocator::Free(
      pointer, PlasmaAllocator::GetFootprintLimit() - 256 * sizeof(size_t));

  int socket = ConnectOrListenIpcSock(socket_name_, true);
  // TODO(pcm): Check return value.
  RAY_CHECK(socket >= 0);

  loop_->AddFileEvent(socket, kEventLoopRead, [this, socket](int events) {
    this->store_->ConnectClient(socket);
  });
  loop_->Start();

  Shutdown();
#ifdef _WINSOCKAPI_
  WSACleanup();
#endif
}

void PlasmaStoreRunner::Stop() {
  if (loop_) {
    loop_->Stop();
  } else {
    RAY_LOG(ERROR) << "Expected loop_ to be non-NULL; this may be a bug";
  }
}

void PlasmaStoreRunner::Shutdown() {
  loop_->Shutdown();
  loop_ = nullptr;
  store_ = nullptr;
}

std::unique_ptr<PlasmaStoreRunner> plasma_store_runner;

}  // namespace plasma
