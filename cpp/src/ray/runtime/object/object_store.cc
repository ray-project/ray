
#include "object_store.h"

#include <memory>
#include <utility>

namespace ray {

const int getTimeoutMs = 1000;

void ObjectStore::put(const UniqueId &objectId, std::shared_ptr<msgpack::sbuffer> data) {
  putRaw(objectId, data);
}

std::shared_ptr< msgpack::sbuffer> ObjectStore::get(const UniqueId &objectId, int timeoutMs) {
  return getRaw(objectId, timeoutMs);
}

std::vector<std::shared_ptr<msgpack::sbuffer>> ObjectStore::get(const std::vector<UniqueId> &objects,
            int timeoutMs) {
  return getRaw(objects, timeoutMs);
}
}  // namespace ray