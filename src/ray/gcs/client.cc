#include "ray/gcs/client.h"

#include "ray/gcs/redis_context.h"

namespace ray {

namespace gcs {

AsyncGcsClient::AsyncGcsClient() {}

AsyncGcsClient::~AsyncGcsClient() {}

Status AsyncGcsClient::Connect(const std::string &address, int port) {
  context_.reset(new RedisContext());
  RAY_RETURN_NOT_OK(context_->Connect(address, port));
  object_table_.reset(new ObjectTable(context_, this));
  task_table_.reset(new TaskTable(context_, this));
  return Status::OK();
}

Status Attach(plasma::EventLoop &event_loop) {
  // TODO(pcm): Implement this via
  // context()->AttachToEventLoop(event loop)
  return Status::OK();
}

ObjectTable &AsyncGcsClient::object_table() {
  return *object_table_;
}

TaskTable &AsyncGcsClient::task_table() {
  return *task_table_;
}

FunctionTable &AsyncGcsClient::function_table() {
  return *function_table_;
}

ClassTable &AsyncGcsClient::class_table() {
  return *class_table_;
}

}  // namespace gcs

}  // namespace ray
