
#include <algorithm>
#include <chrono>
#include <list>
#include <thread>

#include <ray/api/ray_exception.h>
#include "local_mode_object_store.h"

namespace ray {
namespace api {

void LocalModeObjectStore::PutRaw(const ObjectID &object_id,
                                  std::shared_ptr<msgpack::sbuffer> data) {
  absl::MutexLock lock(&data_mutex_);
  if (object_pool_.find(object_id) != object_pool_.end()) {
    throw RayException("object already exist");
  }
  object_pool_.emplace(object_id, data);
}

std::shared_ptr<msgpack::sbuffer> LocalModeObjectStore::GetRaw(const ObjectID &object_id,
                                                               int timeout_ms) {
  const std::vector<ObjectID> objects = {object_id};
  Wait(objects, 1, timeout_ms);

  std::shared_ptr<msgpack::sbuffer> object;

  absl::MutexLock lock(&data_mutex_);

  auto iterator = object_pool_.find(object_id);
  if (iterator == object_pool_.end()) {
    throw RayException("Can not find object in local buffer");
  }

  return iterator->second;
}

std::vector<std::shared_ptr<msgpack::sbuffer>> LocalModeObjectStore::GetRaw(
    const std::vector<ObjectID> &objects, int timeout_ms) {
  WaitResult waitResult = Wait(objects, objects.size(), timeout_ms);
  if (waitResult.unready.size() != 0) {
    throw RayException("Objects are not all ready");
  }

  std::vector<std::shared_ptr<msgpack::sbuffer>> result;
  absl::MutexLock lock(&data_mutex_);

  for (auto it = objects.begin(); it != objects.end(); it++) {
    auto iterator = object_pool_.find(*it);
    if (iterator == object_pool_.end()) {
      throw RayException("Can not find object in local buffer");
    }
    result.push_back(iterator->second);
  }

  return result;
}

WaitResult LocalModeObjectStore::Wait(const std::vector<ObjectID> &objects,
                                      int num_objects, int64_t timeout_ms) {
  static const int GET_CHECK_INTERVAL_MS = 100;
  std::list<ObjectID> ready;
  std::list<ObjectID> unready(objects.begin(), objects.end());
  int readyCnt = 0;
  int remainingTime = timeout_ms;
  bool firstCheck = true;
  while (readyCnt < num_objects && (timeout_ms < 0 || remainingTime > 0)) {
    if (!firstCheck) {
      long sleepTime = timeout_ms < 0 ? GET_CHECK_INTERVAL_MS
                                      : std::min(remainingTime, GET_CHECK_INTERVAL_MS);
      std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
      remainingTime -= sleepTime;
    }
    for (auto it = unready.begin(); it != unready.end(); it++) {
      absl::MutexLock lock(&data_mutex_);
      if (object_pool_.find(*it) != object_pool_.end()) {
        readyCnt += 1;
        ready.push_back(*it);
        it = unready.erase(it);
      } else {
      }
    }
    firstCheck = false;
  }

  std::vector<ObjectID> readyVector{std::begin(ready), std::end(ready)};
  std::vector<ObjectID> unreadyVector{std::begin(unready), std::end(unready)};
  WaitResult result(std::move(readyVector), std::move(unreadyVector));
  return result;
}
}  // namespace api
}  // namespace ray