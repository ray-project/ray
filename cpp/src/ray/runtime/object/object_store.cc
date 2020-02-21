
#include "object_store.h"

#include <memory>
#include <utility>

namespace ray {

const int fetchSize = 10000;
const int getTimeoutMs = 1000;

void ObjectStore::put(const UniqueId &objectId, std::vector< ::ray::blob> &&data) {
  putRaw(objectId, std::forward<std::vector< ::ray::blob> >(data));
}

del_unique_ptr< ::ray::blob> ObjectStore::get(const UniqueId &objectId, int timeoutMs) {
  return getRaw(objectId, timeoutMs);
}
}  // namespace ray