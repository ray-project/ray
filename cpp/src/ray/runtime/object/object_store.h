
#pragma once

#include <memory>

#include <ray/api/wait_result.h>
#include <msgpack.hpp>

namespace ray { namespace api {

extern const int getTimeoutMs;

class ObjectStore {
 private:
 public:
  void put(const ObjectID &objectId, std::shared_ptr<msgpack::sbuffer> data);

  std::shared_ptr<msgpack::sbuffer> get(const ObjectID &objectId,
                                        int timeoutMs = getTimeoutMs);

  std::vector<std::shared_ptr<msgpack::sbuffer>> get(const std::vector<ObjectID> &objects,
                                                     int timeoutMs = getTimeoutMs);

  virtual void putRaw(const ObjectID &objectId,
                      std::shared_ptr<msgpack::sbuffer> data) = 0;

  virtual void del(const ObjectID &objectId) = 0;

  virtual std::shared_ptr<msgpack::sbuffer> getRaw(const ObjectID &objectId,
                                                   int timeoutMs) = 0;

  virtual std::vector<std::shared_ptr<msgpack::sbuffer>> getRaw(
      const std::vector<ObjectID> &objects, int timeoutMs) = 0;

  virtual WaitResultInternal wait(const std::vector<ObjectID> &objects, int num_objects,
                                  int64_t timeout_ms) = 0;

  virtual ~ObjectStore(){};
};
}  }// namespace ray::api