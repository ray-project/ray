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

#include "ray/core_worker/common.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "ray/rpc/client_call.h"

namespace ray {

class ExperimentalChannelManager {
 public:
  ExperimentalChannelManager(
      std::shared_ptr<plasma::PlasmaClient> plasma_client,
      std::function<std::shared_ptr<ExperimentalChannelReaderInterface>(
          const NodeID &node_id)> raylet_client_factory
      )
      : plasma_client_(plasma_client),
      raylet_client_factory_(raylet_client_factory),
      io_work_(io_service_),
      client_call_manager_(new rpc::ClientCallManager(io_service_)) {
    boost::thread::attributes io_thread_attrs;
    io_thread_ = boost::thread(io_thread_attrs, [this]() { RunIOService(); });
  }

  std::unique_ptr<rpc::ClientCallManager> &GetClientCallManager() {
    return client_call_manager_;
  }

  void RegisterCrossNodeWriterChannel(const ObjectID &channel_id, const NodeID &node_id);

  void RegisterCrossNodeReaderChannel(const ObjectID &channel_id,
                                      int64_t num_readers,
                                      const ObjectID &local_reader_channel_id);

  void HandlePushExperimentalChannelValue(
      const rpc::PushExperimentalChannelValueRequest &request,
      rpc::PushExperimentalChannelValueReply *reply);

 private:
  struct WriterChannelInfo {
    WriterChannelInfo(const NodeID &reader_node_id) : reader_node_id(reader_node_id) {}

    const NodeID reader_node_id;
    std::thread send_thread;
  };

  struct ReaderChannelInfo {
    ReaderChannelInfo(int64_t num_readers, const ObjectID &local_reader_channel_id)
        : num_readers(num_readers), local_reader_channel_id(local_reader_channel_id) {}

    const int64_t num_readers;
    const ObjectID local_reader_channel_id;
  };

  void RunIOService();

  void PollWriterChannelAndCopyToReader(
      const ObjectID &object_id,
      std::shared_ptr<ExperimentalChannelReaderInterface> reader_client,
      std::shared_ptr<rpc::PushExperimentalChannelValueRequest> request);

  std::shared_ptr<plasma::PlasmaClient> plasma_client_;
  std::function<std::shared_ptr<ExperimentalChannelReaderInterface>(
      const NodeID &node_id)>
      raylet_client_factory_;

  instrumented_io_context io_service_;
  boost::asio::io_service::work io_work_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
  boost::thread io_thread_;

  absl::flat_hash_map<ObjectID, WriterChannelInfo> write_channels_;
  absl::flat_hash_map<ObjectID, ReaderChannelInfo> read_channels_;
};

}  // namespace ray
