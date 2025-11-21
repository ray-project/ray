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

#include <memory>
#include <string>

#include "ray/ray_syncer/ray_syncer_bidi_reactor.h"
#include "ray/ray_syncer/ray_syncer_bidi_reactor_base.h"

namespace ray::syncer {

using ClientBidiReactor =
    grpc::ClientBidiReactor<RaySyncMessageBatch, RaySyncMessageBatch>;

/// Reactor for gRPC client side. It defines the client's specific behavior for a
/// streaming call.
class RayClientBidiReactor : public RaySyncerBidiReactorBase<ClientBidiReactor> {
 private:
  // Enables `make_shared` inside of the class without exposing a public constructor.
  struct PrivateTag {};

 public:
  // Static factory method to create `RayClientBidiReactor` instances.
  //
  // All instances of `RaySyncerBidiReactorBase` must be heap-allocated shared_ptrs, so
  // this is the only publicly-exposed method of construction.
  static std::shared_ptr<RayClientBidiReactor> Create(
      const std::string &remote_node_id,
      const std::string &local_node_id,
      instrumented_io_context &io_context,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
      std::function<void(RaySyncerBidiReactor *, bool)> cleanup_cb,
      std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub,
      size_t max_batch_size,
      uint64_t max_batch_delay_ms);

  // Constructor, which is enforced to be private via `PrivateTag`.
  RayClientBidiReactor(
      PrivateTag,
      const std::string &remote_node_id,
      const std::string &local_node_id,
      instrumented_io_context &io_context,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
      std::function<void(RaySyncerBidiReactor *, bool)> cleanup_cb,
      std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub,
      size_t max_batch_size,
      uint64_t max_batch_delay_ms);

  ~RayClientBidiReactor() override = default;

 private:
  void DoDisconnect() override;
  void OnDone(const grpc::Status &status) override;

  /// grpc callback context
  grpc::ClientContext client_context_;

  std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub_;
};

}  // namespace ray::syncer
