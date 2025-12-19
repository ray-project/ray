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

#pragma once

#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs_rpc_client/accessors/actor_info_accessor_interface.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

class FakeActorInfoAccessor : public gcs::ActorInfoAccessorInterface {
 public:
  FakeActorInfoAccessor() = default;

  ~FakeActorInfoAccessor() {}

  // Stub implementations for interface methods not used by this test
  void AsyncGet(const ActorID &,
                const gcs::OptionalItemCallback<rpc::ActorTableData> &) override {}
  void AsyncGetAllByFilter(const std::optional<ActorID> &,
                           const std::optional<JobID> &,
                           const std::optional<std::string> &,
                           const gcs::MultiItemCallback<rpc::ActorTableData> &,
                           int64_t = -1) override {}
  void AsyncGetByName(const std::string &,
                      const std::string &,
                      const gcs::OptionalItemCallback<rpc::ActorTableData> &,
                      int64_t = -1) override {}
  Status SyncGetByName(const std::string &,
                       const std::string &,
                       rpc::ActorTableData &,
                       rpc::TaskSpec &) override {
    return Status::OK();
  }
  Status SyncListNamedActors(
      bool,
      const std::string &,
      std::vector<std::pair<std::string, std::string>> &) override {
    return Status::OK();
  }
  void AsyncReportActorOutOfScope(const ActorID &,
                                  uint64_t,
                                  const gcs::StatusCallback &,
                                  int64_t = -1) override {}
  void AsyncRegisterActor(const TaskSpecification &task_spec,
                          const gcs::StatusCallback &callback,
                          int64_t = -1) override {
    async_register_actor_callback_ = callback;
  }
  void AsyncRestartActorForLineageReconstruction(const ActorID &,
                                                 uint64_t,
                                                 const gcs::StatusCallback &,
                                                 int64_t = -1) override {}
  Status SyncRegisterActor(const TaskSpecification &) override { return Status::OK(); }
  void AsyncKillActor(
      const ActorID &, bool, bool, const gcs::StatusCallback &, int64_t = -1) override {}
  void AsyncCreateActor(
      const TaskSpecification &task_spec,
      const rpc::ClientCallback<rpc::CreateActorReply> &callback) override {
    async_create_actor_callback_ = callback;
  }

  void AsyncSubscribe(
      const ActorID &actor_id,
      const gcs::SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const gcs::StatusCallback &done) override {
    auto callback_entry = std::make_pair(actor_id, subscribe);
    callback_map_.emplace(actor_id, subscribe);
    subscribe_finished_callback_map_[actor_id] = done;
    actor_subscribed_times_[actor_id]++;
  }

  void AsyncUnsubscribe(const ActorID &) override {}
  void AsyncResubscribe() override {}
  bool IsActorUnsubscribed(const ActorID &) override { return false; }

  bool ActorStateNotificationPublished(const ActorID &actor_id,
                                       const rpc::ActorTableData &actor_data) {
    auto it = callback_map_.find(actor_id);
    if (it == callback_map_.end()) return false;
    auto actor_state_notification_callback = it->second;
    auto copied = actor_data;
    actor_state_notification_callback(actor_id, std::move(copied));
    return true;
  }

  bool CheckSubscriptionRequested(const ActorID &actor_id) {
    return callback_map_.find(actor_id) != callback_map_.end();
  }

  // Mock the logic of subscribe finished. see `ActorInfoAccessor::AsyncSubscribe`
  bool ActorSubscribeFinished(const ActorID &actor_id,
                              const rpc::ActorTableData &actor_data) {
    auto subscribe_finished_callback_it = subscribe_finished_callback_map_.find(actor_id);
    if (subscribe_finished_callback_it == subscribe_finished_callback_map_.end()) {
      return false;
    }

    auto copied = actor_data;
    if (!ActorStateNotificationPublished(actor_id, std::move(copied))) {
      return false;
    }

    auto subscribe_finished_callback = subscribe_finished_callback_it->second;
    subscribe_finished_callback(Status::OK());
    // Erase callback when actor subscribe is finished.
    subscribe_finished_callback_map_.erase(subscribe_finished_callback_it);
    return true;
  }

  absl::flat_hash_map<ActorID, gcs::SubscribeCallback<ActorID, rpc::ActorTableData>>
      callback_map_;
  absl::flat_hash_map<ActorID, gcs::StatusCallback> subscribe_finished_callback_map_;
  absl::flat_hash_map<ActorID, uint32_t> actor_subscribed_times_;

  // Callbacks for AsyncCreateActor and AsyncRegisterActor
  rpc::ClientCallback<rpc::CreateActorReply> async_create_actor_callback_;
  gcs::StatusCallback async_register_actor_callback_;
};

}  // namespace gcs
}  // namespace ray
