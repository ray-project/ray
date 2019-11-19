
#pragma once

#include <memory>

#include <ray/api/blob.h>
#include <ray/api/uniqueId.h>
#include <ray/util/type_util.h>

#include "object_interface.h"

namespace ray {

extern const int fetchSize;
extern const int getTimeoutMs;

class ObjectProxy {
 private:
  std::unique_ptr<ObjectInterface> _objectInterface;

 public:
  ObjectProxy(std::unique_ptr<ObjectInterface> store);

  void put(const UniqueId &objectId, std::vector< ::ray::blob> &&data);

  del_unique_ptr< ::ray::blob> get(const UniqueId &objectId,
                                   int timeoutMs = getTimeoutMs);

  WaitResult wait(const UniqueId *ids, int count, int minNumReturns, int timeoutMs);

  void fetch(const UniqueId &objectId);
};
}