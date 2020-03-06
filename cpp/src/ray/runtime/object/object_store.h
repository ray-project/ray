
#pragma once

#include <memory>

#include <msgpack.hpp>
#include <ray/api/uniqueId.h>
#include <ray/api/wait_result.h>

namespace ray {

extern const int getTimeoutMs;

class ObjectStore {
 private:
 public:
  void put(const UniqueId &objectId, std::shared_ptr<msgpack::sbuffer> data);

  std::shared_ptr< msgpack::sbuffer> get(const UniqueId &objectId,
                                   int timeoutMs = getTimeoutMs);

  std::vector<std::shared_ptr<msgpack::sbuffer>> get(const std::vector<UniqueId> &objects,
            int timeoutMs = getTimeoutMs);

  virtual void putRaw(const UniqueId &objectId, std::shared_ptr<msgpack::sbuffer> data) = 0;

  virtual void del(const UniqueId &objectId) = 0;

  virtual std::shared_ptr< msgpack::sbuffer> getRaw(const UniqueId &objectId,
                                              int timeoutMs) = 0;

  virtual std::vector<std::shared_ptr<msgpack::sbuffer>> getRaw(const std::vector<UniqueId> &objects,
                                              int timeoutMs) = 0;

  virtual WaitResultInternal wait(const std::vector<UniqueId> &objects, int num_objects, int64_t timeout_ms) = 0;

  virtual ~ObjectStore(){};
};
}  // namespace ray