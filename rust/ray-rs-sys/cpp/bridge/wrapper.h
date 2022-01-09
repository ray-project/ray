
#pragma once

#include <msgpack.hpp>

// #include <ray/api.h>
#include "api.h"
#include "ray/core_worker/core_worker_options.h"
#include "rust/cxx.h"
#include "ray/core_worker/core_worker.h"

namespace ray {

// using ray::core::CoreWorker;

rust::Vec<uint8_t> GetRaw(std::unique_ptr<ObjectID> id);

std::unique_ptr<ObjectID> PutRaw(rust::Vec<uint8_t> data);

void LogDebug(rust::Str str);

void LogInfo(rust::Str str);

std::unique_ptr<std::string> ObjectIDString(std::unique_ptr<ObjectID> id);

std::unique_ptr<ObjectID> StringObjectID(const std::string &string);

// bool IsInitialized();
// /// Shutdown Ray runtime.
// void Shutdown();

// void InitAsLocal();
//
// struct Config {
//   std::string my_string;
//   uint64_t my_int;
//   MSGPACK_DEFINE(my_string, my_int);
// };
//
// using Uint64ObjectRef = ray::ObjectRef<uint64_t>;
//
// std::unique_ptr<Uint64ObjectRef> PutUint64(const uint64_t obj);
//
// std::shared_ptr<uint64_t> GetUint64(const std::unique_ptr<Uint64ObjectRef> obj_ref);
//
// using StringObjectRef = ray::ObjectRef<std::string>;
//
// std::unique_ptr<StringObjectRef> PutString(const std::string &obj);
//
// std::shared_ptr<std::string> GetString(const std::unique_ptr<StringObjectRef> obj_ref);


// void PutAndGetConfig();

}  // namespace ray
