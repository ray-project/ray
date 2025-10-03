// Copyright 2017 The Ray Authors.
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

#include <memory>

// Forward declarations for accessor interfaces
namespace ray {
namespace gcs {

class ActorInfoAccessorInterface;
class JobInfoAccessorInterface;
class NodeInfoAccessorInterface;
class NodeResourceInfoAccessorInterface;
class ErrorInfoAccessorInterface;
class TaskInfoAccessorInterface;
class WorkerInfoAccessorInterface;
class PlacementGroupInfoAccessorInterface;
class InternalKVAccessorInterface;
class RuntimeEnvAccessorInterface;
class AutoscalerStateAccessorInterface;
class PublisherAccessorInterface;
class GcsClientContext;

/// \class AccessorFactoryInterface
/// Factory interface for creating GCS accessor instances.
/// This enables dependency injection and makes the code more testable
class AccessorFactoryInterface {
 public:
  virtual ~AccessorFactoryInterface() = default;

  /// Create an ActorInfoAccessor instance.
  /// \param context The GCS client implementation.
  /// \return A unique pointer to the created ActorInfoAccessor.
  virtual std::unique_ptr<ActorInfoAccessorInterface> CreateActorInfoAccessor(
      GcsClientContext *context) = 0;

  /// Create a JobInfoAccessor instance.
  /// \param context The GCS client context.
  /// \return A unique pointer to the created JobInfoAccessor.
  virtual std::unique_ptr<JobInfoAccessorInterface> CreateJobInfoAccessor(
      GcsClientContext *context) = 0;

  /// Create a NodeInfoAccessor instance.
  /// \param context The GCS client context.
  /// \return A unique pointer to the created NodeInfoAccessor.
  virtual std::unique_ptr<NodeInfoAccessorInterface> CreateNodeInfoAccessor(
      GcsClientContext *context) = 0;

  /// Create a NodeResourceInfoAccessor instance.
  /// \param context The GCS client context.
  /// \return A unique pointer to the created NodeResourceInfoAccessor.
  virtual std::unique_ptr<NodeResourceInfoAccessorInterface>
  CreateNodeResourceInfoAccessor(GcsClientContext *context) = 0;

  /// Create an ErrorInfoAccessor instance.
  /// \param context The GCS client context.
  /// \return A unique pointer to the created ErrorInfoAccessor.
  virtual std::unique_ptr<ErrorInfoAccessorInterface> CreateErrorInfoAccessor(
      GcsClientContext *context) = 0;

  /// Create a TaskInfoAccessor instance.
  /// \param context The GCS client context.
  /// \return A unique pointer to the created TaskInfoAccessor.
  virtual std::unique_ptr<TaskInfoAccessorInterface> CreateTaskInfoAccessor(
      GcsClientContext *context) = 0;

  /// Create a WorkerInfoAccessor instance.
  /// \param context The GCS client context.
  /// \return A unique pointer to the created WorkerInfoAccessor.
  virtual std::unique_ptr<WorkerInfoAccessorInterface> CreateWorkerInfoAccessor(
      GcsClientContext *context) = 0;

  /// Create a PlacementGroupInfoAccessor instance.
  /// \param context The GCS client context.
  /// \return A unique pointer to the created PlacementGroupInfoAccessor.
  virtual std::unique_ptr<PlacementGroupInfoAccessorInterface>
  CreatePlacementGroupInfoAccessor(GcsClientContext *context) = 0;

  /// Create an InternalKVAccessor instance.
  /// \param context The GCS client context.
  /// \return A unique pointer to the created InternalKVAccessor.
  virtual std::unique_ptr<InternalKVAccessorInterface> CreateInternalKVAccessor(
      GcsClientContext *context) = 0;

  /// Create a RuntimeEnvAccessor instance.
  /// \param context The GCS client context.
  /// \return A unique pointer to the created RuntimeEnvAccessor.
  virtual std::unique_ptr<RuntimeEnvAccessorInterface> CreateRuntimeEnvAccessor(
      GcsClientContext *context) = 0;

  /// Create an AutoscalerStateAccessor instance.
  /// \param context The GCS client context.
  /// \return A unique pointer to the created AutoscalerStateAccessor.
  virtual std::unique_ptr<AutoscalerStateAccessorInterface> CreateAutoscalerStateAccessor(
      GcsClientContext *context) = 0;

  /// Create a PublisherAccessor instance.
  /// \param context The GCS client context.
  /// \return A unique pointer to the created PublisherAccessor.
  virtual std::unique_ptr<PublisherAccessorInterface> CreatePublisherAccessor(
      GcsClientContext *context) = 0;
};

}  // namespace gcs
}  // namespace ray
