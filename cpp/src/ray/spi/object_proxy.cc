
#include "object_proxy.h"

#include <memory>
#include <utility>

namespace ray {

const int fetchSize = 10000;
const int getTimeoutMs = 1000;

ObjectProxy::ObjectProxy(std::unique_ptr<ObjectInterface> store) {
  _objectInterface = std::move(store);
}

void ObjectProxy::put(const UniqueId &objectId, std::vector< ::ray::blob> &&data) {
  _objectInterface->put(objectId, std::forward<std::vector< ::ray::blob> >(data));
}

del_unique_ptr< ::ray::blob> ObjectProxy::get(const UniqueId &objectId,
                                                   int timeoutMs) {
  return _objectInterface->get(objectId, timeoutMs);
}

WaitResult ObjectProxy::wait(const UniqueId *ids, int count, int minNumReturns,
                                  int timeoutMs) {
  return _objectInterface->wait(ids, count, minNumReturns, timeoutMs);
}

void ObjectProxy::fetch(const UniqueId &objectId) { return _objectInterface->fetch(objectId); }
}