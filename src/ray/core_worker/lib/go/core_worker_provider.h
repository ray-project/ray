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

#ifndef RAY_CORE_WORKER_LIB_GO_CORE_WORKER_PROVIDER_H
#define RAY_CORE_WORKER_LIB_GO_CORE_WORKER_PROVIDER_H

#include <memory>
#include <stdexcept>

#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_process.h"

namespace ray {
namespace go {

// ============================================================================
// CoreWorker Provider Interface - Supports dependency injection and testing
// ============================================================================

/**
 * @brief Abstract interface for providing CoreWorker instances.
 *
 * This interface allows for dependency injection, making it possible to
 * mock CoreWorker in unit tests without requiring full initialization.
 */
class ICoreWorkerProvider {
 public:
  virtual ~ICoreWorkerProvider() = default;

  /**
   * @brief Get the CoreWorker instance.
   * @return Reference to the CoreWorker.
   */
  virtual core::CoreWorker &GetCoreWorker() = 0;

  /**
   * @brief Check if CoreWorker is initialized.
   * @return true if initialized, false otherwise.
   */
  virtual bool IsInitialized() const = 0;
};

// ============================================================================
// Default Implementation - Uses CoreWorkerProcess singleton
// ============================================================================

/**
 * @brief Default CoreWorker provider that uses the global singleton.
 *
 * This is the production implementation that retrieves the CoreWorker
 * from the CoreWorkerProcess singleton.
 */
class DefaultCoreWorkerProvider : public ICoreWorkerProvider {
 public:
  /**
   * @brief Get the CoreWorker instance from the singleton.
   * @return Reference to the CoreWorker.
   */
  core::CoreWorker &GetCoreWorker() override {
    return ray::core::CoreWorkerProcess::GetCoreWorker();
  }

  /**
   * @brief Check if CoreWorker is initialized.
   * @return true (assumes CoreWorkerProcess is properly initialized).
   */
  bool IsInitialized() const override {
    // In production, we assume CoreWorkerProcess is initialized
    // More sophisticated checks could be added if needed
    return true;
  }
};

// ============================================================================
// Mock Implementation - For unit testing
// ============================================================================

/**
 * @brief Mock CoreWorker provider for unit testing.
 *
 * This provider allows injecting a mock or fake CoreWorker instance
 * for testing without requiring full CoreWorker initialization.
 */
class MockCoreWorkerProvider : public ICoreWorkerProvider {
 public:
  /**
   * @brief Set the mock CoreWorker instance.
   * @param worker Pointer to the mock CoreWorker (must outlive this provider).
   */
  void SetCoreWorker(core::CoreWorker *worker) { mock_worker_ = worker; }

  /**
   * @brief Get the mock CoreWorker instance.
   * @return Reference to the mock CoreWorker.
   * @throws std::runtime_error if no mock worker has been set.
   */
  core::CoreWorker &GetCoreWorker() override {
    if (mock_worker_ == nullptr) {
      throw std::runtime_error("Mock worker not set");
    }
    return *mock_worker_;
  }

  /**
   * @brief Check if a mock CoreWorker has been set.
   * @return true if mock worker is set, false otherwise.
   */
  bool IsInitialized() const override { return mock_worker_ != nullptr; }

 private:
  core::CoreWorker *mock_worker_ = nullptr;
};

// ============================================================================
// Global Registry - Runtime switching of provider implementation
// ============================================================================

/**
 * @brief Global registry for CoreWorker providers.
 *
 * This singleton registry allows switching between different provider
 * implementations at runtime, enabling dependency injection in production
 * and testing scenarios.
 */
class CoreWorkerProviderRegistry {
 public:
  /**
   * @brief Get the singleton instance.
   * @return Reference to the registry instance.
   */
  static CoreWorkerProviderRegistry &Instance() {
    static CoreWorkerProviderRegistry instance;
    return instance;
  }

  /**
   * @brief Set the provider implementation.
   * @param provider Shared pointer to the provider (takes ownership).
   */
  void SetProvider(std::shared_ptr<ICoreWorkerProvider> provider) {
    provider_ = provider;
  }

  /**
   * @brief Get the current provider.
   * @return Reference to the current provider.
   * @throws std::runtime_error if no provider has been set.
   */
  ICoreWorkerProvider &GetProvider() {
    if (provider_ == nullptr) {
      throw std::runtime_error("CoreWorker provider not set");
    }
    return *provider_;
  }

  /**
   * @brief Convenience method to get CoreWorker directly.
   * @return Reference to the CoreWorker.
   */
  core::CoreWorker &GetCoreWorker() { return GetProvider().GetCoreWorker(); }

  /**
   * @brief Check if provider is initialized.
   * @return true if provider is set and initialized, false otherwise.
   */
  bool IsInitialized() const {
    return provider_ != nullptr && provider_->IsInitialized();
  }

 private:
  CoreWorkerProviderRegistry()
      : provider_(std::make_shared<DefaultCoreWorkerProvider>()) {}

  std::shared_ptr<ICoreWorkerProvider> provider_;
};

}  // namespace go
}  // namespace ray

#endif  // RAY_CORE_WORKER_LIB_GO_CORE_WORKER_PROVIDER_H
