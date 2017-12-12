#include "ray/gcs/client.h"

#include "ray/gcs/redis_context.h"

namespace ray {

namespace gcs {

AsyncGCSClient::AsyncGCSClient() {}

AsyncGCSClient::~AsyncGCSClient() {}

Status AsyncGCSClient::Connect(const std::string& address, int port) {
  context_.reset(new RedisContext());
  RETURN_NOT_OK(context_->Connect(address, port));
  object_table_.reset(new ObjectTable(context_));
  task_table_.reset(new TaskTable(context_));
  return Status::OK();
}

Status Attach(plasma::EventLoop& event_loop) {
  // TODO(pcm): Implement this via
  // context()->AttachToEventLoop(event loop)
  return Status::OK();
}

ObjectTable& AsyncGCSClient::object_table() {
  return *object_table_;
}

TaskTable& AsyncGCSClient::task_table() {
  return *task_table_;
}

FunctionTable& AsyncGCSClient::function_table() {
  return *function_table_;
}

ClassTable& AsyncGCSClient::class_table() {
  return *class_table_;
}

}  // namespace gcs

}  // namespace ray
