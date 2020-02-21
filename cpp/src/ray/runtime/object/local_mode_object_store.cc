
#include <algorithm>
#include <chrono>
#include <iostream>
#include <thread>

#include "../../util/blob_util.h"
#include "local_mode_object_store.h"

namespace ray {

void LocalModeObjectStore::putRaw(const UniqueId &objectId,
                                  std::vector< ::ray::blob> &&data) {
  _dataMutex.lock();
  if (_data.find(objectId) != _data.end()) {
    // TODO: throw an exception
  }
  ::ray::blob bb = blob_merge(std::forward<std::vector< ::ray::blob> >(data));
  _data.emplace(objectId, std::move(bb));
  _dataMutex.unlock();
}

void LocalModeObjectStore::del(const UniqueId &objectId) {}

del_unique_ptr< ::ray::blob> LocalModeObjectStore::getRaw(const UniqueId &objectId,
                                                          int timeoutMs) {
  waitInternal(&objectId, 1, 1, -1);

  ::ray::blob *ret = nullptr;
  _dataMutex.lock();

  if (_data.find(objectId) != _data.end()) {
    ret = new ::ray::blob(_data.at(objectId));
  } else {
    std::cout << "Can not find object in local buffer" << std::endl;
  }
  _dataMutex.unlock();

  del_unique_ptr< ::ray::blob> pRet(
      ret, [](::ray::blob *p) { std::default_delete< ::ray::blob>()(p); });

  return pRet;
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

WaitResult LocalModeObjectStore::wait(const UniqueId *ids, int count, int minNumReturns,
                                      int timeoutMs) {
  return WaitResult();
}
}  // namespace ray