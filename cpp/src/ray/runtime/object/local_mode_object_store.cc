
#include <algorithm>
#include <chrono>
#include <list>
#include <thread>

#include "local_mode_object_store.h"

namespace ray { namespace api {

void LocalModeObjectStore::putRaw(const ObjectID &objectId,
                                  std::shared_ptr<msgpack::sbuffer> data) {
  _dataMutex.lock();
  if (_data.find(objectId) != _data.end()) {
    throw "object already exist";
  }
  _data.emplace(objectId, data);
  _dataMutex.unlock();
}

void LocalModeObjectStore::del(const ObjectID &objectId) {}

std::shared_ptr<msgpack::sbuffer> LocalModeObjectStore::getRaw(const ObjectID &objectId,
                                                               int timeoutMs) {
  const std::vector<ObjectID> objects = {objectId};
  waitInternal(objects, 1, -1);

  std::shared_ptr<msgpack::sbuffer> ret;
  _dataMutex.lock();

  if (_data.find(objectId) != _data.end()) {
    ret = _data.at(objectId);
  } else {
    throw "Can not find object in local buffer";
  }
  _dataMutex.unlock();

  return ret;
}

std::vector<std::shared_ptr<msgpack::sbuffer>> LocalModeObjectStore::getRaw(
    const std::vector<ObjectID> &objects, int timeoutMs) {
  WaitResultInternal waitResult = waitInternal(objects, objects.size(), timeoutMs);
  if (waitResult.remains.size() != 0) {
    throw "Objects are not all ready";
  }

  std::vector<std::shared_ptr<msgpack::sbuffer>> result;
  _dataMutex.lock();

  for (auto it = objects.begin(); it != objects.end(); it++) {
    if (_data.find(*it) != _data.end()) {
      result.push_back(_data.at(*it));
    } else {
      throw "Can not find object in local buffer";
    }
  }
  _dataMutex.unlock();

  return result;
}

WaitResultInternal LocalModeObjectStore::waitInternal(
    const std::vector<ObjectID> &objects, int num_objects, int64_t timeout_ms) {
  static const int GET_CHECK_INTERVAL_MS = 100;
  std::list<ObjectID> readys;
  std::list<ObjectID> remains(objects.begin(), objects.end());
  int ready = 0;
  int remainingTime = timeout_ms;
  bool firstCheck = true;
  while (ready < num_objects && (timeout_ms < 0 || remainingTime > 0)) {
    if (!firstCheck) {
      long sleepTime = timeout_ms < 0 ? GET_CHECK_INTERVAL_MS
                                      : std::min(remainingTime, GET_CHECK_INTERVAL_MS);
      std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
      remainingTime -= sleepTime;
    }
    for (auto it = remains.begin(); it != remains.end(); it++) {
      _dataMutex.lock();
      if (_data.find(*it) != _data.end()) {
        ready += 1;
        readys.push_back(*it);
        it = remains.erase(it);
      } else {
      }
      _dataMutex.unlock();
    }
    firstCheck = false;
  }

  std::vector<ObjectID> readysVector{std::begin(readys), std::end(readys)};
  std::vector<ObjectID> readysRemains{std::begin(remains), std::end(remains)};
  WaitResultInternal result(std::move(readysVector), std::move(readysVector));
  return result;
}

WaitResultInternal LocalModeObjectStore::wait(const std::vector<ObjectID> &objects,
                                              int num_objects, int64_t timeout_ms) {
  return waitInternal(objects, num_objects, timeout_ms);
}
}  }// namespace ray::api