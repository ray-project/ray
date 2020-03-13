
#include "object_store.h"

#include <memory>
#include <utility>

namespace ray { namespace api {

const int getTimeoutMs = 1000;

void ObjectStore::put(const ObjectID &objectId, std::shared_ptr<msgpack::sbuffer> data) {
  putRaw(objectId, data);
}

std::shared_ptr<msgpack::sbuffer> ObjectStore::get(const ObjectID &objectId,
                                                   int timeoutMs) {
  return getRaw(objectId, timeoutMs);
}

std::vector<std::shared_ptr<msgpack::sbuffer>> ObjectStore::get(
    const std::vector<ObjectID> &objects, int timeoutMs) {
  return getRaw(objects, timeoutMs);
}
}  }// namespace ray::api