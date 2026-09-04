// Copyright 2025 The Ray Authors.
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

#ifndef RAY_CORE_WORKER_LIB_GO_OBJECT_STORE_PROVIDER_H_
#define RAY_CORE_WORKER_LIB_GO_OBJECT_STORE_PROVIDER_H_

#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_process.h"

namespace ray {
namespace go {

/**
 * Interface for accessing CoreWorker instance.
 * Enables dependency injection for testability.
 */
class IObjectStoreProvider {
 public:
  virtual ~IObjectStoreProvider() = default;

  /**
   * Get the CoreWorker instance.
   * @return Reference to CoreWorker
   * @throws std::runtime_error if not initialized
   */
  virtual core::CoreWorker& GetCoreWorker() = 0;

  /**
   * Check if the provider is initialized.
   * @return true if initialized, false otherwise
   */
  virtual bool IsInitialized() const = 0;
};

/**
 * Default implementation that retrieves CoreWorker from CoreWorkerProcess.
 * Used in production code.
 */
class DefaultObjectStoreProvider : public IObjectStoreProvider {
 public:
  core::CoreWorker& GetCoreWorker() override {
    return ray::core::CoreWorkerProcess::GetCoreWorker();
  }

  bool IsInitialized() const override {
    return ray::core::CoreWorkerProcess::IsInitialized();
  }
};

/**
 * Mock implementation for unit testing.
 * Allows injecting a mock CoreWorker instance.
 */
class MockObjectStoreProvider : public IObjectStoreProvider {
 public:
  /**
   * Set the mock CoreWorker instance.
   * @param worker Pointer to mock CoreWorker (must outlive the provider)
   */
  void SetCoreWorker(core::CoreWorker* worker) {
    mock_worker_ = worker;
  }

  core::CoreWorker& GetCoreWorker() override {
    if (!mock_worker_) {
      throw std::runtime_error("Mock CoreWorker not set");
    }
    return *mock_worker_;
  }

  bool IsInitialized() const override {
    return mock_worker_ != nullptr;
  }

 private:
  core::CoreWorker* mock_worker_ = nullptr;
};

}  // namespace go
}  // namespace ray

#endif  // RAY_CORE_WORKER_LIB_GO_OBJECT_STORE_PROVIDER_H_
