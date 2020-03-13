
#pragma once

#include <unordered_map>

#include <ray/core.h>

#include "object_store.h"

namespace ray { namespace api {

class LocalModeObjectStore : public ObjectStore {
 private:
  std::unordered_map<ObjectID, std::shared_ptr<msgpack::sbuffer>> _data;

  std::mutex _dataMutex;

  WaitResult WaitInternal(const std::vector<ObjectID> &objects, int num_objects,
                                  int64_t timeout_ms);

 public:
  void PutRaw(const ObjectID &objectId, std::shared_ptr<msgpack::sbuffer> data);

  void Del(const ObjectID &objectId);

  std::shared_ptr<msgpack::sbuffer> GetRaw(const ObjectID &objectId, int timeoutMs);

  std::vector<std::shared_ptr<msgpack::sbuffer>> GetRaw(
      const std::vector<ObjectID> &objects, int timeoutMs);

  WaitResult Wait(const std::vector<ObjectID> &objects, int num_objects,
                          int64_t timeout_ms);
};

}  }// namespace ray::api