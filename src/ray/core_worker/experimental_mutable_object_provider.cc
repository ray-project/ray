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

#include "absl/strings/str_format.h"

namespace ray {
namespace core {
namespace experimental {

MutableObjectProvider::MutableObjectProvider(
    std::shared_ptr<plasma::PlasmaClientInterface> plasma, RayletFactory factory)
    : plasma_(plasma), raylet_client_factory_(factory) {}

MutableObjectProvider::~MutableObjectProvider() {
  for (std::unique_ptr<boost::asio::executor_work_guard<
           boost::asio::io_context::executor_type>> &io_work : io_works_) {
    io_work->reset();
  }
  RAY_CHECK(object_manager_.SetErrorAll().code() == StatusCode::OK);

  for (std::unique_ptr<std::thread> &io_thread : io_threads_) {
    RAY_CHECK(io_thread->joinable());
    io_thread->join();
  }
}

void MutableObjectProvider::RegisterWriterChannel(const ObjectID &object_id,
                                                  const NodeID *node_id) {
  {
    std::unique_ptr<plasma::MutableObject> object;
    RAY_CHECK_OK(plasma_->GetExperimentalMutableObject(object_id, &object));
    RAY_CHECK_OK(
        object_manager_.RegisterChannel(object_id, std::move(object), /*reader=*/false));
    // `object` is now a nullptr.
  }

  if (node_id) {
    // Start a thread that repeatedly listens for values on this object and then sends
    // them via RPC to the remote reader.
    io_contexts_.push_back(std::make_unique<instrumented_io_context>());
    instrumented_io_context &io_context = *io_contexts_.back();
    io_works_.push_back(
        std::make_unique<
            boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
            io_context.get_executor()));
    client_call_managers_.push_back(std::make_unique<rpc::ClientCallManager>(io_context));
    std::shared_ptr<MutableObjectReaderInterface> reader =
        raylet_client_factory_(*node_id, *client_call_managers_.back());
    RAY_CHECK(reader);
    // TODO(jhumphri): Extend this to support multiple channels. Currently, we must have
    // one thread per channel because the thread blocks on the channel semaphore.

    io_context.post(
        [this, &io_context, object_id, reader]() {
          PollWriterClosure(io_context, object_id, reader);
        },
        "experimental::MutableObjectProvider.PollWriter");
    io_threads_.push_back(std::make_unique<std::thread>(
        &MutableObjectProvider::RunIOContext, this, std::ref(io_context)));
  }
}

void MutableObjectProvider::RegisterReaderChannel(const ObjectID &object_id) {
  std::unique_ptr<plasma::MutableObject> object;
  RAY_CHECK_OK(plasma_->GetExperimentalMutableObject(object_id, &object));
  RAY_CHECK_OK(
      object_manager_.RegisterChannel(object_id, std::move(object), /*reader=*/true));
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
  {
    const ObjectID writer_object_id = ObjectID::FromBinary(request.writer_object_id());
    absl::MutexLock guard(&remote_writer_object_to_local_reader_lock_);
    auto it = remote_writer_object_to_local_reader_.find(writer_object_id);
    RAY_CHECK(it != remote_writer_object_to_local_reader_.end());
    info = it->second;
  }
  size_t data_size = request.data_size();
  size_t metadata_size = request.metadata_size();

  // Copy both the data and metadata to a local channel.
  std::shared_ptr<Buffer> data;
  const uint8_t *metadata_ptr =
      reinterpret_cast<const uint8_t *>(request.data().data()) + request.data_size();
  RAY_CHECK_OK(object_manager_.WriteAcquire(info.local_object_id,
                                            data_size,
                                            metadata_ptr,
                                            metadata_size,
                                            info.num_readers,
                                            data));
  RAY_CHECK(data);

  size_t total_size = data_size + metadata_size;
  // The buffer has the data immediately followed by the metadata. `WriteAcquire()`
  // above checks that the buffer size is at least `total_size`.
  memcpy(data->Data(), request.data().data(), total_size);
  RAY_CHECK_OK(object_manager_.WriteRelease(info.local_object_id));
}

bool MutableObjectProvider::ReaderChannelRegistered(const ObjectID &object_id) {
  return object_manager_.ReaderChannelRegistered(object_id);
}

bool MutableObjectProvider::WriterChannelRegistered(const ObjectID &object_id) {
  return object_manager_.WriterChannelRegistered(object_id);
}

Status MutableObjectProvider::WriteAcquire(const ObjectID &object_id,
                                           int64_t data_size,
                                           const uint8_t *metadata,
                                           int64_t metadata_size,
                                           int64_t num_readers,
                                           std::shared_ptr<Buffer> &data) {
  return object_manager_.WriteAcquire(
      object_id, data_size, metadata, metadata_size, num_readers, data);
}

Status MutableObjectProvider::WriteRelease(const ObjectID &object_id) {
  return object_manager_.WriteRelease(object_id);
}

Status MutableObjectProvider::ReadAcquire(const ObjectID &object_id,
                                          std::shared_ptr<RayObject> &result) {
  return object_manager_.ReadAcquire(object_id, result);
}

Status MutableObjectProvider::ReadRelease(const ObjectID &object_id) {
  return object_manager_.ReadRelease(object_id);
}

Status MutableObjectProvider::SetError(const ObjectID &object_id) {
  return object_manager_.SetError(object_id);
}

void MutableObjectProvider::PollWriterClosure(
    instrumented_io_context &io_context,
    const ObjectID &object_id,
    std::shared_ptr<MutableObjectReaderInterface> reader) {
  std::shared_ptr<RayObject> object;
  Status status = object_manager_.ReadAcquire(object_id, object);
  // Check if the thread returned from ReadAcquire() because the process is exiting, not
  // because there is something to read.
  if (status.code() == StatusCode::IOError) {
    // The process is exiting.
    return;
  }
  RAY_CHECK_EQ(static_cast<int>(status.code()), static_cast<int>(StatusCode::OK));

  RAY_CHECK(object->GetData());
  RAY_CHECK(object->GetMetadata());

  reader->PushMutableObject(
      object_id,
      object->GetData()->Size(),
      object->GetMetadata()->Size(),
      object->GetData()->Data(),
      [this, &io_context, object_id, reader](const Status &status,
                                             const rpc::PushMutableObjectReply &reply) {
        RAY_CHECK_OK(object_manager_.ReadRelease(object_id));

        io_context.post(
            [this, &io_context, object_id, reader]() {
              PollWriterClosure(io_context, object_id, reader);
            },
            "experimental::MutableObjectProvider.PollWriter");
      });
}

void MutableObjectProvider::RunIOContext(instrumented_io_context &io_context) {
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
  io_context.run();
  RAY_LOG(INFO) << "Core worker channel io service stopped.";
}

}  // namespace experimental
}  // namespace core
}  // namespace ray
