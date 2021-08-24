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

#include "ray/object_manager/ownership_based_object_directory.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_client/service_based_accessor.h"
#include "ray/gcs/gcs_client/service_based_gcs_client.h"
#include "ray/pubsub/mock_pubsub.h"

namespace ray {

class MockNodeInfoAccessor : public gcs::NodeInfoAccessor {
 public:
  MockNodeInfoAccessor() {}
  ~MockNodeInfoAccessor() = default;

  MOCK_METHOD2(RegisterSelf, Status(const rpc::GcsNodeInfo &local_node_info,
                                    const gcs::StatusCallback &callback));
  MOCK_METHOD0(UnregisterSelf, Status());

  MOCK_CONST_METHOD0(GetSelfId, const NodeID &());

  MOCK_CONST_METHOD0(GetSelfInfo, const rpc::GcsNodeInfo &());

  MOCK_METHOD2(AsyncRegister, Status(const rpc::GcsNodeInfo &node_info,
                                     const gcs::StatusCallback &callback));

  MOCK_METHOD2(AsyncUnregister,
               Status(const NodeID &node_id, const gcs::StatusCallback &callback));

  MOCK_METHOD1(AsyncGetAll,
               Status(const gcs::MultiItemCallback<rpc::GcsNodeInfo> &callback));

  MOCK_METHOD2(AsyncSubscribeToNodeChange,
               Status(const gcs::SubscribeCallback<NodeID, rpc::GcsNodeInfo> &subscribe,
                      const gcs::StatusCallback &done));

  MOCK_CONST_METHOD2(Get, boost::optional<rpc::GcsNodeInfo>(const NodeID &node_id, bool filter_dead_nodes));

  MOCK_CONST_METHOD0(GetAll, const std::unordered_map<NodeID, rpc::GcsNodeInfo> &());

  MOCK_CONST_METHOD1(IsRemoved, bool(const NodeID &node_id));

  MOCK_METHOD2(AsyncReportHeartbeat,
               Status(const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
                      const gcs::StatusCallback &callback));

  MOCK_METHOD1(AsyncResubscribe, void(bool is_pubsub_server_restarted));

  MOCK_METHOD1(AsyncGetInternalConfig,
               Status(const gcs::OptionalItemCallback<std::string> &callback));
};

class MockGcsClient : public gcs::GcsClient {
 public:
  MockGcsClient(gcs::GcsClientOptions options, MockNodeInfoAccessor *node_info_accessor) : gcs::GcsClient(options) {
    node_accessor_.reset(node_info_accessor);
  }

  gcs::NodeInfoAccessor &Nodes() {
    RAY_CHECK(node_accessor_ != nullptr);
    return *node_accessor_;
  }
};

class OwnershipBasedObjectDirectoryTesst : public ::testing::Test {
 public:
  OwnershipBasedObjectDirectoryTesst()
      : options_("", 1, ""),
        node_info_accessor_(new MockNodeInfoAccessor()),
        gcs_client_mock_(new MockGcsClient(options_, node_info_accessor_)),
        subscriber_(std::make_shared<mock_pubsub::MockSubscriber>()) {
    //   obod_(io_service_, gcs_client_mock_, subscriber_, [this](const ObjectID
    //   &object_id, const rpc::ErrorType &error_type) {
    //     MarkAsFailed(object_id, error_type);
    //   }) {
    // gcs_client_mock_->Init(node_info_accessor_);
  }

  void MarkAsFailed(const ObjectID &object_id, const rpc::ErrorType &error_type) {
    RAY_LOG(ERROR) << "SANG-TODO";
  }

  instrumented_io_context io_service_;
  gcs::GcsClientOptions options_;
  // MockNodeInfoAccessor *node_info_accessor_;
  // std::shared_ptr<gcs::GcsClient> gcs_client_mock_;
  std::shared_ptr<mock_pubsub::MockSubscriber> subscriber_;
  //   OwnershipBasedObjectDirectory obod_;
};

TEST_F(OwnershipBasedObjectDirectoryTesst, TestLocationUpdateBatch) {
  // RAY_LOG(ERROR) << "SANG-TODO " << obod_.DebugString();
}

}  // namespace ray
