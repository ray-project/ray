
#pragma once

#include <unordered_map>

#include "../native_ray_runtime.h"
#include "object_store.h"

namespace ray {
namespace api {

class NativeObjectStore : public ObjectStore {
 public:
  std::vector<bool> Wait(const std::vector<ObjectID> &ids, int num_objects,
                         int timeout_ms);

  void AddLocalReference(const std::string &id);

  void RemoveLocalReference(const std::string &id);

 private:
  void PutRaw(std::shared_ptr<msgpack::sbuffer> data, ObjectID *object_id);

  void PutRaw(std::shared_ptr<msgpack::sbuffer> data, const ObjectID &object_id);

  std::shared_ptr<msgpack::sbuffer> GetRaw(const ObjectID &object_id, int timeout_ms);

  std::vector<std::shared_ptr<msgpack::sbuffer>> GetRaw(const std::vector<ObjectID> &ids,
                                                        int timeout_ms);
  void CheckException(const std::string &meta_str, const std::string &data_str);
};

}  // namespace api
}  // namespace ray