
#include <algorithm>
#include <chrono>
#include <iostream>
#include <thread>

#include "local_mode_object_store.h"

namespace ray {

void LocalModeObjectStore::putRaw(const UniqueId &objectId,
                                  std::shared_ptr<msgpack::sbuffer> data) {
  _dataMutex.lock();
  if (_data.find(objectId) != _data.end()) {
    // TODO: throw an exception
  }
  // ::ray::blob bb = blob_merge(std::forward<std::vector< ::ray::blob> >(data));
  // std::shared_ptr<msgpack::sbuffer> buf(new msgpack::sbuffer());
  // buf->write(data.data(), data.size());
  _data.emplace(objectId, data);
  _dataMutex.unlock();
}

void LocalModeObjectStore::del(const UniqueId &objectId) {}

std::shared_ptr< msgpack::sbuffer> LocalModeObjectStore::getRaw(const UniqueId &objectId,
                                                          int timeoutMs) {
  waitInternal(&objectId, 1, 1, -1);

  std::shared_ptr<msgpack::sbuffer> ret;
  _dataMutex.lock();

  if (_data.find(objectId) != _data.end()) {
    ret = _data.at(objectId);
  } else {
    std::cout << "Can not find object in local buffer" << std::endl;
  }
  _dataMutex.unlock();

  return ret;
}

std::vector<std::shared_ptr<msgpack::sbuffer>> LocalModeObjectStore::getRaw(const std::vector<UniqueId> &objects,
                                              int timeoutMs) {
  return std::vector<std::shared_ptr<msgpack::sbuffer>>();
}

void LocalModeObjectStore::waitInternal(const UniqueId *ids, int count, int minNumReturns,
                                        int timeoutMs) {
  static const int GET_CHECK_INTERVAL_MS = 100;
  int ready = 0;
  int remainingTime = timeoutMs;
  bool firstCheck = true;
  while (ready < minNumReturns && (timeoutMs < 0 || remainingTime > 0)) {
    if (!firstCheck) {
      long sleepTime = timeoutMs < 0 ? GET_CHECK_INTERVAL_MS
                                     : std::min(remainingTime, GET_CHECK_INTERVAL_MS);
      std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
      remainingTime -= sleepTime;
    }
    ready = 0;
    for (int i = 0; i < count; i++) {
      UniqueId uid = ids[i];
      _dataMutex.lock();
      if (_data.find(uid) != _data.end()) {
        ready += 1;
      }
      _dataMutex.unlock();
    }
    firstCheck = false;
  }
}

WaitResultInternal LocalModeObjectStore::wait(const std::vector<UniqueId> &objects, int num_objects, int64_t timeout_ms) {
  return WaitResultInternal();
}
}  // namespace ray