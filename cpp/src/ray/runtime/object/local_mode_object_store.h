
#pragma once

#include <unordered_map>

#include <ray/api/uniqueId.h>
#include <ray/core.h>

#include "object_store.h"

namespace ray {

class LocalModeObjectStore : public ObjectStore {
 private:
  std::unordered_map<UniqueId, std::shared_ptr<msgpack::sbuffer>> _data;

  std::mutex _dataMutex;

  void waitInternal(const UniqueId *ids, int count, int minNumReturns, int timeoutMs);

 public:
  void putRaw(const UniqueId &objectId, std::shared_ptr<msgpack::sbuffer> data);

  void del(const UniqueId &objectId);

  std::shared_ptr< msgpack::sbuffer> getRaw(const UniqueId &objectId, int timeoutMs);

  WaitResult wait(const UniqueId *ids, int count, int minNumReturns, int timeoutMs);
};

}  // namespace ray