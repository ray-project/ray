
#pragma once

#include <vector>

#include <ray/api/Blob.h>
#include <ray/api/UniqueId.h>
#include <ray/util/type-util.h>

namespace ray {

typedef struct WaitResult_s {
  UniqueId *readys;
  UniqueId *remains;
  int readyNum;
  int remainNum;
} WaitResult;

class ObjectInterface {
 public:
  virtual void put(const UniqueId &objectId, std::vector< ::ray::blob> &&data) = 0;

  virtual void release(const UniqueId &objectId) = 0;

  virtual del_unique_ptr< ::ray::blob> get(const UniqueId &objectId,
                                           int timeoutMs) = 0;

  virtual WaitResult wait(const UniqueId *ids, int count, int minNumReturns,
                          int timeoutMs) = 0;

  virtual void fetch(const UniqueId &objectId) = 0;

  virtual ~ObjectInterface(){};
};

}  // namespace ray
