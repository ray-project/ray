
#pragma once

#include "ray/api.h"
#include "ray/core_worker/core_worker_options.h"
#include <msgpack.hpp>
#include "rust/cxx.h"

namespace ray {

void InitAsLocal();

rust::Vec<uint8_t> GetRaw(std::unique_ptr<ObjectID>id);

using Uint64ObjectRef = ray::ObjectRef<uint64_t>;

std::unique_ptr<Uint64ObjectRef> PutUint64(const uint64_t obj);

std::shared_ptr<uint64_t> GetUint64(const std::unique_ptr<Uint64ObjectRef> obj_ref);

using StringObjectRef = ray::ObjectRef<std::string>;

std::unique_ptr<StringObjectRef> PutString(const std::string &obj);

std::shared_ptr<std::string> GetString(const std::unique_ptr<StringObjectRef> obj_ref);

struct Config {
  std::string my_string;
  uint64_t my_int;
  MSGPACK_DEFINE(my_string, my_int);
};

void PutAndGetConfig();

void LogDebug(rust::Str str);

void LogInfo(rust::Str str);

}
