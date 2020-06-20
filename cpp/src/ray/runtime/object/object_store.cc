
#include "object_store.h"

#include <memory>
#include <utility>

namespace ray {
namespace api {

void ObjectStore::Put(const ObjectID &object_id, std::shared_ptr<msgpack::sbuffer> data) {
  PutRaw(object_id, data);
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