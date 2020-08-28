
#include "object_store.h"

#include <memory>
#include <utility>

namespace ray {
namespace api {

void ObjectStore::Put(std::shared_ptr<msgpack::sbuffer> data, ObjectID *object_id) {
  PutRaw(data, object_id);
}

void ObjectStore::Put(std::shared_ptr<msgpack::sbuffer> data, const ObjectID &object_id) {
  PutRaw(data, object_id);
}

std::shared_ptr<msgpack::sbuffer> ObjectStore::Get(const ObjectID &object_id,
                                                   int timeout_ms) {
  return GetRaw(object_id, timeout_ms);
}

std::vector<std::shared_ptr<msgpack::sbuffer>> ObjectStore::Get(
    const std::vector<ObjectID> &ids, int timeout_ms) {
  return GetRaw(ids, timeout_ms);
}
}  // namespace api
}  // namespace ray