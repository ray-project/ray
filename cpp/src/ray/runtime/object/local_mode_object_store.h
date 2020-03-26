
#pragma once

#include <unordered_map>
#include "absl/synchronization/mutex.h"
#include "ray/core.h"

#include "object_store.h"

namespace ray {
namespace api {

class LocalModeObjectStore : public ObjectStore {
 public:
  WaitResult Wait(const std::vector<ObjectID> &objects, int num_objects,
                  int64_t timeout_ms);

 private:
  void PutRaw(const ObjectID &object_id, std::shared_ptr<msgpack::sbuffer> data);

  std::shared_ptr<msgpack::sbuffer> GetRaw(const ObjectID &object_id, int timeout_ms);

  std::vector<std::shared_ptr<msgpack::sbuffer>> GetRaw(
      const std::vector<ObjectID> &objects, int timeout_ms);

  std::unordered_map<ObjectID, std::shared_ptr<msgpack::sbuffer>> object_pool_;

  absl::Mutex data_mutex_;
};

}  // namespace api
}  // namespace ray