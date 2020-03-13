
#pragma once

#include <memory>

#include <ray/api/wait_result.h>
#include <msgpack.hpp>

namespace ray {
namespace api {

extern const int getTimeoutMs;

class ObjectStore {
 private:
 public:
  void Put(const ObjectID &objectId, std::shared_ptr<msgpack::sbuffer> data);

  std::shared_ptr<msgpack::sbuffer> Get(const ObjectID &objectId,
                                        int timeoutMs = getTimeoutMs);

  std::vector<std::shared_ptr<msgpack::sbuffer>> Get(const std::vector<ObjectID> &objects,
                                                     int timeoutMs = getTimeoutMs);

  virtual void PutRaw(const ObjectID &objectId,
                      std::shared_ptr<msgpack::sbuffer> data) = 0;

  virtual void Del(const ObjectID &objectId) = 0;

  virtual std::shared_ptr<msgpack::sbuffer> GetRaw(const ObjectID &objectId,
                                                   int timeoutMs) = 0;

  virtual std::vector<std::shared_ptr<msgpack::sbuffer>> GetRaw(
      const std::vector<ObjectID> &objects, int timeoutMs) = 0;

  virtual WaitResult Wait(const std::vector<ObjectID> &objects, int num_objects,
                          int64_t timeout_ms) = 0;

  virtual ~ObjectStore(){};
};
}  // namespace api
}  // namespace ray