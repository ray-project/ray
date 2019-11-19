
#include "object_interface_mock.h"
#include "../util/blob_util.h"

namespace ray {

void ObjectInterfaceMock::put(const UniqueId &objectId, std::vector< ::ray::blob> &&data) {
  if (_data.find(objectId) != _data.end()) {
    // TODO: throw an exception
  }
  ::ray::blob bb = blob_merge(std::forward<std::vector< ::ray::blob> >(data));
  _data.emplace(objectId, std::move(bb));
}

void ObjectInterfaceMock::release(const UniqueId &objectId) {}

del_unique_ptr< ::ray::blob> ObjectInterfaceMock::get(const UniqueId &objectId,
                                                      int timeoutMs) {
  ::ray::blob *ret = nullptr;

  if (_data.find(objectId) != _data.end()) {
    ret = new ::ray::blob(_data.at(objectId));
  }
  
  del_unique_ptr< ::ray::blob> pRet(
      ret, [](::ray::blob *p) { std::default_delete< ::ray::blob>()(p); });

  return pRet;
}

WaitResult ObjectInterfaceMock::wait(const UniqueId *ids, int count, int minNumReturns,
                                      int timeoutMs) {
  return WaitResult();
}

void ObjectInterfaceMock::fetch(const UniqueId &objectId) {}
}