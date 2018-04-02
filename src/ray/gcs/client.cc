#include "ray/gcs/client.h"

#include "ray/gcs/redis_context.h"

namespace ray {

namespace gcs {

AsyncGcsClient::AsyncGcsClient(const ClientID &client_id) {
  context_.reset(new RedisContext());
  client_table_.reset(new ClientTable(context_, this, client_id));
  object_table_.reset(new ObjectTable(context_, this));
  task_table_.reset(new TaskTable(context_, this));
  raylet_task_table_.reset(new raylet::TaskTable(context_, this));
  task_reconstruction_log_.reset(new TaskReconstructionLog(context_, this));
  heartbeat_table_.reset(new HeartbeatTable(context_, this));
}

AsyncGcsClient::AsyncGcsClient() : AsyncGcsClient(ClientID::from_random()) {}

AsyncGcsClient::~AsyncGcsClient() {}

Status AsyncGcsClient::Connect(const std::string &address, int port) {
  RAY_RETURN_NOT_OK(context_->Connect(address, port));
  // TODO(swang): Call the client table's Connect() method here. To do this,
  // we need to make sure that we are attached to an event loop first. This
  // currently isn't possible because the aeEventLoop, which we use for
  // testing, requires us to connect to Redis first.
  return Status::OK();
}

Status Attach(plasma::EventLoop &event_loop) {
  // TODO(pcm): Implement this via
  // context()->AttachToEventLoop(event loop)
  return Status::OK();
}

Status AsyncGcsClient::Attach(boost::asio::io_service &io_service) {
  asio_async_client_.reset(new RedisAsioClient(io_service, context_->async_context()));
  asio_subscribe_client_.reset(
      new RedisAsioClient(io_service, context_->subscribe_context()));
  return Status::OK();
}

ObjectTable &AsyncGcsClient::object_table() { return *object_table_; }

TaskTable &AsyncGcsClient::task_table() { return *task_table_; }

raylet::TaskTable &AsyncGcsClient::raylet_task_table() { return *raylet_task_table_; }

TaskReconstructionLog &AsyncGcsClient::task_reconstruction_log() {
  return *task_reconstruction_log_;
}

ClientTable &AsyncGcsClient::client_table() { return *client_table_; }

FunctionTable &AsyncGcsClient::function_table() { return *function_table_; }

ClassTable &AsyncGcsClient::class_table() { return *class_table_; }

HeartbeatTable &AsyncGcsClient::heartbeat_table() { return *heartbeat_table_; }

}  // namespace gcs

}  // namespace ray
