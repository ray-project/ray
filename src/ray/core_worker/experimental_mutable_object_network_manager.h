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

#include "ray/core_worker/common.h"
#include "ray/core_worker/experimental_mutable_object_manager.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/worker/core_worker_client_pool.h"

namespace ray {
namespace core {
namespace experimental {

template <typename T>
class MutableObjectNetworkManager {
 public:
  typedef std::function<std::shared_ptr<MutableObjectReaderInterface>(
      const NodeID &node_id)>
      RayletFactory;

  MutableObjectNetworkManager(std::shared_ptr<T> plasma, const RayletFactory &factory)
      : plasma_(plasma),
        raylet_client_factory_(factory),
        io_work_(io_service_),
        client_call_manager_(std::make_unique<rpc::ClientCallManager>(io_service_)),
        object_manager_(std::make_unique<ray::experimental::MutableObjectManager>()),
        io_thread_([this]() { RunIOService(); }) {}

  ~MutableObjectNetworkManager() {
    io_service_.stop();

    RAY_CHECK(io_thread_.joinable());
    io_thread_.join();
  }

  std::unique_ptr<rpc::ClientCallManager> &client_call_manager() {
    return client_call_manager_;
  }

  /// Registers a writer channel for `object_id` on this node. On each write to this
  /// channel, the write will be sent via RPC to node `node_id`.
  /// \param[in] object_id The ID of the object.
  /// \param[in] node_id The ID of the node to write to.
  void RegisterWriterChannel(const ObjectID &object_id, const NodeID &node_id) {
    {
      std::unique_ptr<plasma::MutableObject> object;
      RAY_CHECK_OK(plasma_->GetExperimentalMutableObject(object_id, &object));
      RAY_CHECK_OK(object_manager_->RegisterWriterChannel(object_id, std::move(object)));
      // `object` is now a nullptr.
    }

    // Start a thread that repeatedly listens for values on this object and then sends
    // them via RPC to the remote reader.
    std::shared_ptr<MutableObjectReaderInterface> reader =
        raylet_client_factory_(node_id);
    RAY_CHECK(reader);
    // TODO(jhumphri): Extend this to support multiple channels. Currently, we must have
    // one thread per channel because the thread blocks on the channel semaphore.
    io_service_.post(
        [this, object_id, reader]() { PollWriterClosure(object_id, reader); },
        "experimental::MutableObjectNetworkManager.PollWriter");
  }

  /// Registers a reader channel for `object_id` on this node.
  /// \param[in] object_id The ID of the object.
  void RegisterReaderChannel(const ObjectID &object_id) {
    std::unique_ptr<plasma::MutableObject> object;
    RAY_CHECK_OK(plasma_->GetExperimentalMutableObject(object_id, &object));
    RAY_CHECK_OK(object_manager_->RegisterReaderChannel(object_id, std::move(object)));
    // `object` is now a nullptr.
  }

  // RPC callback for when a writer pushes a mutable object over the network to a reader
  // on this node.
  void HandlePushMutableObject(const rpc::PushMutableObjectRequest &request,
                               rpc::PushMutableObjectReply *reply) {
    const ObjectID object_id = ObjectID::FromBinary(request.object_id());
    size_t data_size = request.data_size();
    size_t metadata_size = request.metadata_size();

    // Copy both the data and metadata to a local channel.
    std::shared_ptr<Buffer> data;
    const uint8_t *metadata_ptr =
        reinterpret_cast<const uint8_t *>(request.data().data()) + request.data_size();
    // TODO(jhumphri): Set `num_readers` correctly for this local node.
    RAY_CHECK_OK(object_manager_->WriteAcquire(
        object_id, data_size, metadata_ptr, metadata_size, /*num_readers=*/1, data));
    RAY_CHECK(data);

    size_t total_size = data_size + metadata_size;
    // The buffer has the data immediately followed by the metadata. `WriteAcquire()`
    // above checks that the buffer size is at least `total_size`.
    memcpy(data->Data(), request.data().data(), total_size);
    RAY_CHECK_OK(object_manager_->WriteRelease(object_id));
  }

 private:
  void PollWriterClosure(const ObjectID &object_id,
                         std::shared_ptr<MutableObjectReaderInterface> reader) {
    std::shared_ptr<RayObject> object;
    RAY_CHECK_OK(object_manager_->ReadAcquire(object_id, object));

    RAY_CHECK(object->GetData());
    RAY_CHECK(object->GetMetadata());
    reader->PushMutableObject(
        object_id,
        object->GetData()->Size(),
        object->GetMetadata()->Size(),
        object->GetData()->Data(),
        [this, object_id, reader](const Status &status,
                                  const rpc::PushMutableObjectReply &reply) {
          RAY_CHECK_OK(object_manager_->ReadRelease(object_id));
          PollWriterClosure(object_id, reader);
        });
  }

  void RunIOService() {
    // TODO(jhumphri): Decompose this.
#ifndef _WIN32
    // Block SIGINT and SIGTERM so they will be handled by the main thread.
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);
#endif

    SetThreadName("worker.channel_io");
    io_service_.run();
    RAY_LOG(INFO) << "Core worker channel io service stopped.";
  }

  // The plasma store.
  std::shared_ptr<T> plasma_;
  // Creates a function for each object. This object waits for changes on the object and
  // then sends those changes to a remote node via RPC.
  std::function<std::shared_ptr<MutableObjectReaderInterface>(const NodeID &node_id)>
      raylet_client_factory_;

  // Object manager for the mutable objects.
  std::unique_ptr<ray::experimental::MutableObjectManager> object_manager_;

  // Manages RPCs for inter-node communication of mutable objects.
  instrumented_io_context io_service_;
  boost::asio::io_service::work io_work_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
  std::thread io_thread_;
};

}  // namespace experimental
}  // namespace core
}  // namespace ray
