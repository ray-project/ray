
#pragma once

#include <memory>

#include <msgpack.hpp>
#include <ray/api/uniqueId.h>

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
  void put(const UniqueId &objectId, std::shared_ptr<msgpack::sbuffer> data);

  std::shared_ptr< msgpack::sbuffer> get(const UniqueId &objectId,
                                   int timeoutMs = getTimeoutMs);

  virtual void putRaw(const UniqueId &objectId, std::shared_ptr<msgpack::sbuffer> data) = 0;

  virtual void del(const UniqueId &objectId) = 0;

  virtual std::shared_ptr< msgpack::sbuffer> getRaw(const UniqueId &objectId,
                                              int timeoutMs) = 0;

  virtual WaitResult wait(const UniqueId *ids, int count, int minNumReturns,
                          int timeoutMs) = 0;

  virtual ~ObjectStore(){};
};
}  // namespace ray