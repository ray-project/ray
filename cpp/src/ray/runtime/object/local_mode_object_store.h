
#pragma once

#include <unordered_map>

#include <ray/core.h>

#include "object_store.h"

namespace ray { namespace api {

class LocalModeObjectStore : public ObjectStore {
 private:
  std::unordered_map<ObjectID, std::shared_ptr<msgpack::sbuffer>> _data;

  std::mutex _dataMutex;

  WaitResultInternal waitInternal(const std::vector<ObjectID> &objects, int num_objects,
                                  int64_t timeout_ms);

 public:
  void putRaw(const ObjectID &objectId, std::shared_ptr<msgpack::sbuffer> data);

  void del(const ObjectID &objectId);

  std::shared_ptr<msgpack::sbuffer> getRaw(const ObjectID &objectId, int timeoutMs);

  std::vector<std::shared_ptr<msgpack::sbuffer>> getRaw(
      const std::vector<ObjectID> &objects, int timeoutMs);

  WaitResultInternal wait(const std::vector<ObjectID> &objects, int num_objects,
                          int64_t timeout_ms);
};

}  }// namespace ray::api