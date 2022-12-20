// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "io_ray_runtime_context_NativeWorkerContext.h"

#include <jni.h>

#include "jni_utils.h"
#include "ray/common/id.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jint JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetCurrentTaskType(JNIEnv *env,
                                                                         jclass) {
  auto task_spec = CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentTask();
  RAY_CHECK(task_spec) << "Current task is not set.";
  return static_cast<int>(task_spec->GetMessage().type());
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetCurrentTaskId(JNIEnv *env,
                                                                       jclass) {
  const TaskID &task_id =
      CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentTaskID();
  return IdToJavaByteBuffer<TaskID>(env, task_id);
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetCurrentJobId(JNIEnv *env,
                                                                      jclass) {
  const auto &job_id =
      CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentJobID();
  return IdToJavaByteBuffer<JobID>(env, job_id);
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetCurrentWorkerId(JNIEnv *env,
                                                                         jclass) {
  const auto &worker_id =
      CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetWorkerID();
  return IdToJavaByteBuffer<WorkerID>(env, worker_id);
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetCurrentActorId(JNIEnv *env,
                                                                        jclass) {
  const auto &actor_id =
      CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentActorID();
  return IdToJavaByteBuffer<ActorID>(env, actor_id);
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetRpcAddress(JNIEnv *env, jclass) {
  const auto &rpc_address = CoreWorkerProcess::GetCoreWorker().GetRpcAddress();
  return NativeStringToJavaByteArray(env, rpc_address.SerializeAsString());
}

JNIEXPORT jstring JNICALL
Java_io_ray_runtime_context_NativeWorkerContext_nativeGetSerializedRuntimeEnv(JNIEnv *env,
                                                                              jclass) {
  std::string serialized_runtime_env;
  if (CoreWorkerProcess::GetCoreWorker().GetWorkerType() == WorkerType::DRIVER) {
    serialized_runtime_env = CoreWorkerProcess::GetCoreWorker()
                                 .GetJobConfig()
                                 .runtime_env_info()
                                 .serialized_runtime_env();
  } else {
    serialized_runtime_env = CoreWorkerProcess::GetCoreWorker()
                                 .GetWorkerContext()
                                 .GetCurrentSerializedRuntimeEnv();
  }
  return env->NewStringUTF(serialized_runtime_env.c_str());
}

#ifdef __cplusplus
}
#endif
