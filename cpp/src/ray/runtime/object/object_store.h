
#pragma once

#include <memory>

#include <ray/api/blob.h>
#include <ray/api/uniqueId.h>
#include <ray/util/type_util.h>

namespace ray {

extern const int fetchSize;
extern const int getTimeoutMs;

typedef struct WaitResult_s {
  UniqueId *readys;
  UniqueId *remains;
  int readyNum;
  int remainNum;
} WaitResult;

class ObjectStore {
 private:

 public:

  void put(const UniqueId &objectId, std::vector< ::ray::blob> &&data);

  del_unique_ptr< ::ray::blob> get(const UniqueId &objectId,
                                   int timeoutMs = getTimeoutMs);

  virtual void putRaw(const UniqueId &objectId, std::vector< ::ray::blob> &&data) = 0;

  virtual void del(const UniqueId &objectId) = 0;

  virtual del_unique_ptr< ::ray::blob> getRaw(const UniqueId &objectId, int timeoutMs) = 0;

  virtual WaitResult wait(const UniqueId *ids, int count, int minNumReturns, int timeoutMs) = 0;

  virtual ~ObjectStore(){};
};
}  // namespace ray