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

#include "ray/core_worker/experimental_mutable_object_provider.h"

#include <memory>
#include <utility>
#include <vector>

namespace ray {
namespace core {
namespace experimental {

MutableObjectProvider::MutableObjectProvider(
    std::shared_ptr<plasma::PlasmaClientInterface> plasma,
    RayletFactory raylet_client_factory,
    std::function<Status(void)> check_signals)
    : plasma_(std::move(plasma)),
      object_manager_(std::make_shared<ray::experimental::MutableObjectManager>(
          std::move(check_signals))),
      raylet_client_factory_(std::move(raylet_client_factory)) {}

MutableObjectProvider::~MutableObjectProvider() {
  for (std::unique_ptr<boost::asio::executor_work_guard<
           boost::asio::io_context::executor_type>> &io_work : io_works_) {
    io_work->reset();
  }
  RAY_CHECK(object_manager_->SetErrorAll().code() == StatusCode::OK);

  for (std::unique_ptr<std::thread> &io_thread : io_threads_) {
    RAY_CHECK(io_thread->joinable());
    io_thread->join();
  }
}

void MutableObjectProvider::RegisterWriterChannel(
    const ObjectID &writer_object_id, const std::vector<NodeID> &remote_reader_node_ids) {
  {
    std::unique_ptr<plasma::MutableObject> writer_object;
    RAY_CHECK_OK(plasma_->GetExperimentalMutableObject(writer_object_id, &writer_object));
    RAY_CHECK_OK(object_manager_->RegisterChannel(
        writer_object_id, std::move(writer_object), /*reader=*/false));
    // `object` is now a nullptr.
  }

  if (remote_reader_node_ids.empty()) {
    return;
  }

  std::shared_ptr<std::vector<std::shared_ptr<RayletClientInterface>>> remote_readers =
      std::make_shared<std::vector<std::shared_ptr<RayletClientInterface>>>();
  // TODO(sang): Currently, these attributes are not cleaned up.
  // Start a thread that repeatedly listens for values on this object and then sends
  // them via RPC to the remote reader.
  io_contexts_.push_back(std::make_unique<instrumented_io_context>());
  instrumented_io_context &io_context = *io_contexts_.back();
  io_works_.push_back(
      std::make_unique<
          boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
          io_context.get_executor()));

  // Find remote readers.
  for (const auto &node_id : remote_reader_node_ids) {
    // NOTE: Not setting local address because we're not testing compiled graphs with
    // testing_rpc_failure_avoid_intra_node_failures for now.
    client_call_managers_.push_back(std::make_unique<rpc::ClientCallManager>(
        io_context, /*record_stats=*/false, /*local_address=*/"always not local"));
    std::shared_ptr<RayletClientInterface> reader = raylet_client_factory_(node_id);
    remote_readers->push_back(reader);
  }

  // TODO(jhumphri): Extend this to support multiple channels. Currently, we must have
  // one thread per channel because the thread blocks on the channel semaphore.
  // TODO(sang): We currently create a thread per object id. It is not scalable.
  // We should instead just use a pool of threads.
  io_context.post(
      [this,
       &io_context,
       writer_object_id,
       remote_readers = std::move(remote_readers)]() {
        PollWriterClosure(io_context, writer_object_id, remote_readers);
      },
      "experimental::MutableObjectProvider.PollWriter");
  io_threads_.push_back(std::make_unique<std::thread>(
      &MutableObjectProvider::RunIOContext, this, std::ref(io_context)));
}

void MutableObjectProvider::RegisterReaderChannel(const ObjectID &object_id) {
  std::unique_ptr<plasma::MutableObject> object;
  RAY_CHECK_OK(plasma_->GetExperimentalMutableObject(object_id, &object));
  RAY_CHECK_OK(
      object_manager_->RegisterChannel(object_id, std::move(object), /*reader=*/true));
  // `object` is now a nullptr.
}

void MutableObjectProvider::HandleRegisterMutableObject(
    const ObjectID &writer_object_id,
    int64_t num_readers,
    const ObjectID &reader_object_id) {
  absl::MutexLock guard(&remote_writer_object_to_local_reader_lock_);

  LocalReaderInfo info;
  info.num_readers = num_readers;
  info.local_object_id = reader_object_id;
  bool success =
      remote_writer_object_to_local_reader_.insert({writer_object_id, info}).second;
  RAY_CHECK(success);

  RegisterReaderChannel(reader_object_id);
}

void MutableObjectProvider::HandlePushMutableObject(
    const rpc::PushMutableObjectRequest &request, rpc::PushMutableObjectReply *reply) {
  LocalReaderInfo info;
  const ObjectID writer_object_id = ObjectID::FromBinary(request.writer_object_id());
  {
    absl::MutexLock guard(&remote_writer_object_to_local_reader_lock_);
    auto it = remote_writer_object_to_local_reader_.find(writer_object_id);
    RAY_CHECK(it != remote_writer_object_to_local_reader_.end());
    info = it->second;
  }
  size_t total_data_size = request.total_data_size();
  size_t total_metadata_size = request.total_metadata_size();

  uint64_t offset = request.offset();
  uint64_t chunk_size = request.chunk_size();

  uint64_t tmp_written_so_far = 0;
  {
    absl::MutexLock guard(&written_so_far_lock_);

    tmp_written_so_far = written_so_far_[writer_object_id];
    written_so_far_[writer_object_id] += chunk_size;
    if (written_so_far_[writer_object_id] == total_data_size) {
      written_so_far_.erase(written_so_far_.find(writer_object_id));
    }
  }

  std::shared_ptr<Buffer> object_backing_store;
  if (tmp_written_so_far == 0u) {
    // We set `metadata` to nullptr since the metadata is at the end of the object, which
    // we will not have until the last chunk is received.
    RAY_CHECK_OK(object_manager_->WriteAcquire(info.local_object_id,
                                               total_data_size,
                                               /*metadata=*/nullptr,
                                               total_metadata_size,
                                               info.num_readers,
                                               object_backing_store));
  } else {
    RAY_CHECK_OK(object_manager_->GetObjectBackingStore(info.local_object_id,
                                                        total_data_size,
                                                        total_metadata_size,
                                                        object_backing_store));
  }
  RAY_CHECK(object_backing_store);

  memcpy(object_backing_store->Data() + offset, request.data().data(), chunk_size);
  size_t total_written = tmp_written_so_far + chunk_size;
  RAY_CHECK_LE(total_written, total_data_size);
  if (total_written == total_data_size) {
    // Copy the metadata to the end of the object.
    memcpy(object_backing_store->Data() + total_data_size,
           request.metadata().data(),
           total_metadata_size);
    // The entire object has been written, so call `WriteRelease()`.
    RAY_CHECK_OK(object_manager_->WriteRelease(info.local_object_id));
    reply->set_done(true);
  } else {
    reply->set_done(false);
  }
}

Status MutableObjectProvider::WriteAcquire(const ObjectID &object_id,
                                           int64_t data_size,
                                           const uint8_t *metadata,
                                           int64_t metadata_size,
                                           int64_t num_readers,
                                           std::shared_ptr<Buffer> &data,
                                           int64_t timeout_ms) {
  return object_manager_->WriteAcquire(
      object_id, data_size, metadata, metadata_size, num_readers, data, timeout_ms);
}

Status MutableObjectProvider::WriteRelease(const ObjectID &object_id) {
  return object_manager_->WriteRelease(object_id);
}

Status MutableObjectProvider::ReadAcquire(const ObjectID &object_id,
                                          std::shared_ptr<RayObject> &result,
                                          int64_t timeout_ms) {
  return object_manager_->ReadAcquire(object_id, result, timeout_ms);
}

Status MutableObjectProvider::ReadRelease(const ObjectID &object_id) {
  return object_manager_->ReadRelease(object_id);
}

Status MutableObjectProvider::SetError(const ObjectID &object_id) {
  return object_manager_->SetError(object_id);
}

Status MutableObjectProvider::GetChannelStatus(const ObjectID &object_id,
                                               bool is_reader) {
  return object_manager_->GetChannelStatus(object_id, is_reader);
}

void MutableObjectProvider::PollWriterClosure(
    instrumented_io_context &io_context,
    const ObjectID &writer_object_id,
    const std::shared_ptr<std::vector<std::shared_ptr<RayletClientInterface>>>
        &remote_readers) {
  // NOTE: There's only 1 PollWriterClosure at any time in a single thread.
  std::shared_ptr<RayObject> object;
  // The corresponding ReadRelease() will be automatically called when
  // `object` goes out of scope.
  Status status = object_manager_->ReadAcquire(writer_object_id, object);
  // Check if the thread returned from ReadAcquire() because the process is exiting, not
  // because there is something to read.
  if (status.code() == StatusCode::ChannelError) {
    // The process is exiting.
    return;
  }
  RAY_CHECK_EQ(static_cast<int>(status.code()), static_cast<int>(StatusCode::OK));

  RAY_CHECK(object->GetData());
  RAY_CHECK(object->GetMetadata());

  std::shared_ptr<size_t> num_replied = std::make_shared<size_t>(0);
  for (const auto &reader : *remote_readers) {
    reader->PushMutableObject(
        writer_object_id,
        object->GetData()->Size(),
        object->GetMetadata()->Size(),
        object->GetData()->Data(),
        object->GetMetadata()->Data(),
        [this, &io_context, writer_object_id, remote_readers, num_replied](
            const Status &push_object_status, const rpc::PushMutableObjectReply &reply) {
          *num_replied += 1;
          if (!push_object_status.ok()) {
            RAY_LOG(ERROR)
                << "Failed to transfer object to a remote node for an object id "
                << writer_object_id << ". It can cause hang.";
          }

          if (*num_replied == remote_readers->size()) {
            io_context.post(
                [this, &io_context, writer_object_id, remote_readers]() {
                  PollWriterClosure(io_context, writer_object_id, remote_readers);
                },
                "experimental::MutableObjectProvider.PollWriter");
          }
        });
  }
}

void MutableObjectProvider::RunIOContext(instrumented_io_context &io_context) {
// TODO(jhumphri): Decompose this.
#ifndef _WIN32
  // Block SIGINT and SIGTERM so they will be handled by the main thread.
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, SIGINT);
  sigaddset(&mask, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &mask, nullptr);

#endif
  SetThreadName("worker.channel_io");
  io_context.run();
  RAY_LOG(INFO) << "Core worker channel io service stopped.";
}

}  // namespace experimental
}  // namespace core
}  // namespace ray
