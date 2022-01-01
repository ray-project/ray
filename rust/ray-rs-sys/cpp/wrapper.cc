
#include "ray/api.h"
#include "ray/core_worker/core_worker_options.h"
#include "ray/core_worker/core_worker_process.h"
#include "ray/core_worker/core_worker.h"
#include "ray/util/event.h"
#include "ray/util/event_label.h"
#include <msgpack.hpp>
#include "rust/cxx.h"

namespace ray {

rust::Vec<uint8_t> GetRaw(std::unique_ptr<ObjectID> id) {
  core::CoreWorker &core_worker = core::CoreWorkerProcess::GetCoreWorker();
  std::vector<std::shared_ptr<::ray::RayObject>> results;
  std::vector<ObjectID> ids;
  ids.push_back(*id);
  ::ray::Status status = core_worker.Get(ids, 6000, &results);

  rust::Vec<uint8_t> buf;
  size_t size = results[0]->GetData()->Size();

  RAY_LOG(DEBUG) << "Get Size" << size;
  buf.reserve(size);

  // Unfortunately, we can't resize the vector and do a memcpy...
  // memcpy(buf.data(), results[0]->GetData()->Data(), size);

  uint8_t* ray_buf = results[0]->GetData()->Data();

  size_t i;
  for (i = 0; i < size; i++) {
    buf.push_back(ray_buf[i]);
  }
  return buf;
}

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

void LogDebug(rust::Str str) {
  RAY_LOG(DEBUG) << static_cast<std::string>(str);
}

void LogInfo(rust::Str str) {
  RAY_LOG(INFO) << static_cast<std::string>(str);
}
}
