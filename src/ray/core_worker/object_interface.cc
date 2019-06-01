#include "object_interface.h"

namespace ray {

Status CoreWorkerObjectInterface::Put(const Buffer &buffer, const ObjectID *object_id) {
  return Status::OK();
}

Status CoreWorkerObjectInterface::Get(const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms, std::vector<Buffer> *results) {
  return Status::OK();
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &object_ids,
                                       int num_objects, int64_t timeout_ms,
                                       std::vector<bool> *results) {
  return Status::OK();
}

Status CoreWorkerObjectInterface::Delete(const std::vector<ObjectID> &object_ids,
                                         bool local_only, bool delete_creating_tasks) {
  return Status::OK();
}

}  // namespace ray
