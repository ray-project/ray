
#include "api.h"

#include "config_internal.h"
#include "process_helper.h"
#include "ray/core_worker/core_worker.h"

namespace ray {

static bool is_init_ = false;
void SetConfigToWorker() {
  internal::ConfigInternal::Instance().worker_type = core::WorkerType::WORKER;
}

bool ShouldInitWithCallback() {
  return !(internal::ConfigInternal::Instance().run_mode ==
               internal::RunMode::SINGLE_PROCESS ||
           core::CoreWorkerProcess::IsInitialized());
}

void Init(RayConfig &config, core::CoreWorkerOptions::TaskExecutionCallback callback,
          int argc, char **argv) {
  if (!IsInitialized()) {
    internal::ConfigInternal::Instance().Init(config, argc, argv);
    if (ShouldInitWithCallback()) {
      internal::ProcessHelper::GetInstance().RayStart(callback);
    }
    is_init_ = true;
  }
}

void StartWorkerWithCallback(core::CoreWorkerOptions::TaskExecutionCallback callback,
                             int argc, char **argv) {
  RAY_LOG(INFO) << "RUST default worker started.";
  internal::ConfigInternal::Instance().worker_type = core::WorkerType::WORKER;
  RayConfig config;
  Init(config, callback, argc, argv);
  core::CoreWorkerProcess::RunTaskExecutionLoop();
}

void Init(RayConfig &config, int argc, char **argv) {
  if (!IsInitialized()) {
    internal::ConfigInternal::Instance().Init(config, argc, argv);
    // if (ShouldInitWithCallback()) {
    //   internal::ProcessHelper::GetInstance().RayStart(
    //       internal::TaskExecutor::ExecuteTask);
    // }
    is_init_ = true;
  }
}

void Init(RayConfig &config) { Init(config, 0, nullptr); }

void Init() {
  RayConfig config;
  Init(config, 0, nullptr);
}

bool IsInitialized() { return is_init_; }

void Shutdown() {
  // TODO(SongGuyang): Clean the ray runtime.
  if (internal::ConfigInternal::Instance().run_mode == internal::RunMode::CLUSTER) {
    internal::ProcessHelper::GetInstance().RayStop();
  }
  is_init_ = false;
}


rust::Vec<uint8_t> GetRaw(std::unique_ptr<ObjectID> id) {
  RAY_LOG(INFO) << "GETTING RAW" << (*id).Hex();
  core::CoreWorker &core_worker = core::CoreWorkerProcess::GetCoreWorker();
  std::vector<std::shared_ptr<::ray::RayObject>> results;
  std::vector<ObjectID> ids;
  std::vector<bool> res;
  ids.push_back(*id);
  ::ray::Status status1 = core_worker.Wait(ids, 1, 6000, &res, false);
  RAY_LOG(INFO) << "GOT IT!" << (*id).Hex();
  ::ray::Status status = core_worker.Get(ids, 6000, &results);

  if (!status.ok()) {
    RAY_LOG(INFO) << "Get object error: " << status.ToString();
  }

  rust::Vec<uint8_t> buf;
  size_t size = results[0]->GetData()->Size();

  RAY_LOG(INFO) << "Get Size" << size;
  buf.reserve(size);

  // Unfortunately, we can't resize the vector and do a memcpy...
  // memcpy(buf.data(), results[0]->GetData()->Data(), size);

  uint8_t *ray_buf = results[0]->GetData()->Data();

  size_t i;
  for (i = 0; i < size; i++) {
    buf.push_back(ray_buf[i]);
  }
  return buf;
}

std::unique_ptr<ObjectID> PutRaw(rust::Vec<uint8_t> data) {
  RAY_LOG(INFO) << "Putting";
  auto &core_worker = core::CoreWorkerProcess::GetCoreWorker();
  ObjectID object_id;
  RAY_LOG(INFO) << "worker RPC" << core_worker.GetRpcAddress().DebugString();
  RAY_LOG(INFO) << "Putting" << object_id.Hex();
  auto buffer = std::make_shared<::ray::LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(data.data()), data.size(), true);

  RAY_LOG(INFO) << "Putting2" << object_id.Hex();
  auto status = core_worker.Put(
      ::ray::RayObject(buffer, nullptr, std::vector<rpc::ObjectReference>()), {},
      &object_id);
  if (!status.ok()) {
    RAY_LOG(INFO) << "Put object error: " << status.ToString();
  } else {
    RAY_LOG(INFO) << "Put object success: " << status.ToString();
  }
  return std::make_unique<ObjectID>(object_id);
}

void LogDebug(rust::Str str) { RAY_LOG(DEBUG) << static_cast<std::string>(str); }

void LogInfo(rust::Str str) { RAY_LOG(INFO) << static_cast<std::string>(str); }

std::unique_ptr<std::string> ObjectIDString(std::unique_ptr<ObjectID> id) {
  return std::make_unique<std::string>((*id).Binary());
}

std::unique_ptr<ObjectID> StringObjectID(const std::string &string) {
  return std::make_unique<ObjectID>(ObjectID::FromBinary(string));
}

}
