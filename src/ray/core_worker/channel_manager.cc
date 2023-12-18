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

#include "ray/core_worker/channel_manager.h"

#include "src/ray/common/buffer.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

void ExperimentalChannelManager::PollWriterChannelAndCopyToReader(
    const ObjectID &channel_id,
    std::shared_ptr<rpc::CoreWorkerClientInterface> reader_client) {
  std::vector<plasma::ObjectBuffer> objects;
  std::vector<ObjectID> object_ids = {channel_id};

  rpc::PushExperimentalChannelValueRequest request;
  request.set_channel_id(channel_id.Binary());

  RAY_LOG(DEBUG)
      << "PushExperimentalChannelValue, waiting for next value from sender channel "
      << channel_id;
  RAY_CHECK_OK(plasma_client_->Get(object_ids,
                                   /*timeout_ms=*/-1,
                                   &objects,
                                   /*is_from_worker=*/true));

  RAY_CHECK(objects.size() == 1);
  request.set_data_size_bytes(objects[0].data->Size());
  request.set_metadata_size_bytes(objects[0].metadata->Size());
  // NOTE(swang): This assumes that the format of the object is a contiguous
  // buffer of  (data | metadata).
  request.set_data(objects[0].data->Data(),
                   objects[0].data->Size() + objects[0].metadata->Size());

  RAY_LOG(DEBUG) << "PushExperimentalChannelValue, pushing value from sender channel "
                 << channel_id << " to remote reader with worker ID "
                 << WorkerID::FromBinary(reader_client->Addr().worker_id());

  reader_client->PushExperimentalChannelValue(
      request,
      [this, channel_id, reader_client](
          const Status &status, const rpc::PushExperimentalChannelValueReply &reply) {
        RAY_CHECK(reply.success());
        plasma_client_->ExperimentalMutableObjectReadRelease(channel_id);
        PollWriterChannelAndCopyToReader(channel_id, reader_client);
      });
}

void ExperimentalChannelManager::RegisterCrossNodeWriterChannel(
    const ObjectID &channel_id, const ActorID &actor_id) {
  auto inserted = write_channels_.insert({channel_id, WriterChannelInfo(actor_id)});
  RAY_CHECK(inserted.second);

  // Start a thread that repeatedly listens for values on this
  // channel, then sends via RPC to the remote reader.
  auto reader_client = actor_client_factory_(actor_id);
  RAY_CHECK(reader_client);
  std::thread send_thread([this, channel_id, reader_client]() {
    PollWriterChannelAndCopyToReader(channel_id, reader_client);
  });

  inserted.first->second.send_thread = std::move(send_thread);
}

void ExperimentalChannelManager::RegisterCrossNodeReaderChannel(
    const ObjectID &channel_id,
    int64_t num_readers,
    const ObjectID &local_reader_channel_id) {
  RAY_LOG(DEBUG) << "Register cross node reader channel " << channel_id
                 << " to local channel " << local_reader_channel_id;
  auto inserted = read_channels_.insert(
      {channel_id, ReaderChannelInfo(num_readers, local_reader_channel_id)});
  RAY_CHECK(inserted.second);
}

void ExperimentalChannelManager::HandlePushExperimentalChannelValue(
    const rpc::PushExperimentalChannelValueRequest &request,
    rpc::PushExperimentalChannelValueReply *reply) {
  const auto channel_id = ObjectID::FromBinary(request.channel_id());
  RAY_LOG(DEBUG) << "Received PushExperimentalChannelValue, from sender channel "
                 << channel_id;
  auto it = read_channels_.find(channel_id);
  if (it == read_channels_.end()) {
    reply->set_success(false);
    return;
  }
  RAY_LOG(DEBUG)
      << "Received PushExperimentalChannelValue, writing value from sender channel "
      << channel_id << " to local channel " << it->second.local_reader_channel_id
      << " with data size " << request.data_size_bytes() << " and metadata size "
      << request.metadata_size_bytes();

  // Copy the data to a local channel.
  std::shared_ptr<Buffer> plasma_buffer;
  const uint8_t *metadata_ptr = reinterpret_cast<const uint8_t *>(request.data().data()) +
                                request.data_size_bytes();
  RAY_CHECK_OK(plasma_client_->ExperimentalMutableObjectWriteAcquire(
      it->second.local_reader_channel_id,
      request.data_size_bytes(),
      metadata_ptr,
      request.metadata_size_bytes(),
      it->second.num_readers,
      &plasma_buffer));
  std::memcpy(plasma_buffer->Data(), request.data().data(), request.data_size_bytes());
  RAY_CHECK_OK(plasma_client_->ExperimentalMutableObjectWriteRelease(
      it->second.local_reader_channel_id));
  reply->set_success(true);
}

}  // namespace core
}  // namespace ray
