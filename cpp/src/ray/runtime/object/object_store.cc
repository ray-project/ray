
#include "object_store.h"

#include <memory>
#include <utility>

namespace ray {

const int fetchSize = 10000;
const int getTimeoutMs = 1000;

void ObjectStore::put(const UniqueId &objectId, std::shared_ptr<msgpack::sbuffer> data) {
  putRaw(objectId, data);
}

std::shared_ptr< msgpack::sbuffer> ObjectStore::get(const UniqueId &objectId, int timeoutMs) {
  return getRaw(objectId, timeoutMs);
}
}  // namespace ray