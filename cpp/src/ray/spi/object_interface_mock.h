
#pragma once

#include <unordered_map>

#include <ray/api/uniqueId.h>
#include <ray/core.h>
#include <ray/util/type_util.h>

#include "object_interface.h"

namespace ray {

class ObjectInterfaceMock : public ObjectInterface {
 private:
  std::unordered_map<UniqueId, ::ray::blob> _data;

 public:
  void put(const UniqueId &objectId, std::vector< ::ray::blob> &&data);

  void release(const UniqueId &objectId);

  del_unique_ptr< ::ray::blob> get(const UniqueId &objectId, int timeoutMs);

  WaitResult wait(const UniqueId *ids, int count, int minNumReturns, int timeoutMs);

  void fetch(const UniqueId &objectId);
};

}  // namespace ray