
#include "object_store.h"

#include <memory>
#include <utility>

namespace ray {
namespace api {

const int getTimeoutMs = 1000;

void ObjectStore::Put(const ObjectID &objectId, std::shared_ptr<msgpack::sbuffer> data) {
  PutRaw(objectId, data);
}

std::shared_ptr<msgpack::sbuffer> ObjectStore::Get(const ObjectID &objectId,
                                                   int timeoutMs) {
  return GetRaw(objectId, timeoutMs);
}

std::vector<std::shared_ptr<msgpack::sbuffer>> ObjectStore::Get(
    const std::vector<ObjectID> &objects, int timeoutMs) {
  return GetRaw(objects, timeoutMs);
}
}  // namespace api
}  // namespace ray