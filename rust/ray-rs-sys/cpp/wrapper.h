
#pragma once

#include "ray/api.h"
#include "ray/core_worker/core_worker_options.h"
#include <msgpack.hpp>

namespace ray {
  void InitAsLocal() {
    ray::RayConfig config;
    config.local_mode = true;
    ray::Init(config);
  }
  using Uint64ObjectRef = ray::ObjectRef<uint64_t>;

  std::unique_ptr<Uint64ObjectRef> PutUint64(const uint64_t obj) {
    auto ref = Put<uint64_t>(obj);
    ray::internal::GetRayRuntime()->AddLocalReference(ref.ID());
    // this actually requires `UniquePtr` with custom destructor that Rust-side
    // is knowledgeable about;
    // In reality, for the Rust ObjectRef, we can implement all the ref counting in Rust.
    return std::make_unique<Uint64ObjectRef>(ref);
  }

  std::shared_ptr<uint64_t> GetUint64(const std::unique_ptr<Uint64ObjectRef> obj_ref) {
    return Get<uint64_t>(*obj_ref);
  }

  using StringObjectRef = ray::ObjectRef<std::string>;

  std::unique_ptr<StringObjectRef> PutString(const std::string &obj) {
    auto ref = Put<std::string>(obj);
    ray::internal::GetRayRuntime()->AddLocalReference(ref.ID());
    return std::make_unique<StringObjectRef>(ref);
  }

  std::shared_ptr<std::string> GetString(const std::unique_ptr<StringObjectRef> obj_ref) {
    return Get<std::string>(*obj_ref);
  }

  struct Config {
    std::string my_string;
    uint64_t my_int;
    MSGPACK_DEFINE(my_string, my_int);
  };

  void PutAndGetConfig() {
    Config config = { "hello", 42ULL };
    auto ref = Put(config);
    Get(ref);
  }

}
