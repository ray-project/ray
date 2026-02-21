// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include "ray/core_worker/store_provider/memory_store/memory_store.h"

namespace ray::core {

// Creates a CoreWorkerMemoryStore with a thread and a event loop hosting the callbacks.
// Should only be used in CPP tests.
class DefaultCoreWorkerMemoryStoreWithThread : public CoreWorkerMemoryStore {
 public:
  static std::unique_ptr<DefaultCoreWorkerMemoryStoreWithThread> Create() {
    std::unique_ptr<InstrumentedIOContextWithThread> io_context =
        std::make_unique<InstrumentedIOContextWithThread>(
            "DefaultCoreWorkerMemoryStoreWithThread");
    // C++ limitation: std::make_unique cannot be used because std::unique_ptr cannot
    // invoke private constructors.
    return std::unique_ptr<DefaultCoreWorkerMemoryStoreWithThread>(
        new DefaultCoreWorkerMemoryStoreWithThread(std::move(io_context)));
  }

  static std::shared_ptr<DefaultCoreWorkerMemoryStoreWithThread> CreateShared() {
    std::unique_ptr<InstrumentedIOContextWithThread> io_context =
        std::make_unique<InstrumentedIOContextWithThread>(
            "DefaultCoreWorkerMemoryStoreWithThread");
    // C++ limitation: std::make_shared cannot be used because std::shared_ptr cannot
    // invoke private constructors.
    return std::shared_ptr<DefaultCoreWorkerMemoryStoreWithThread>(
        new DefaultCoreWorkerMemoryStoreWithThread(std::move(io_context)));
  }

  ~DefaultCoreWorkerMemoryStoreWithThread() { io_context_->Stop(); }

 private:
  explicit DefaultCoreWorkerMemoryStoreWithThread(
      std::unique_ptr<InstrumentedIOContextWithThread> io_context)
      : CoreWorkerMemoryStore(io_context->GetIoService()),
        io_context_(std::move(io_context)) {}

  std::unique_ptr<InstrumentedIOContextWithThread> io_context_;
};

}  // namespace ray::core
