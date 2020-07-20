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

#include "io_ray_runtime_task_NativeTaskExecutor.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "jni_utils.h"
#include "ray/raylet_client/raylet_client.h"

#ifdef __cplusplus
extern "C" {
#endif

using ray::ClientID;

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_task_NativeTaskExecutor_nativePrepareCheckpoint(JNIEnv *env, jclass) {
  auto &core_worker = ray::CoreWorkerProcess::GetCoreWorker();
  const auto &actor_id = core_worker.GetWorkerContext().GetCurrentActorID();
  const auto &task_spec = core_worker.GetWorkerContext().GetCurrentTask();
  RAY_CHECK(task_spec->IsActorTask());
  ActorCheckpointID checkpoint_id;
  auto status = core_worker.PrepareActorCheckpoint(actor_id, &checkpoint_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  jbyteArray result = env->NewByteArray(checkpoint_id.Size());
  env->SetByteArrayRegion(result, 0, checkpoint_id.Size(),
                          reinterpret_cast<const jbyte *>(checkpoint_id.Data()));
  return result;
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_task_NativeTaskExecutor_nativeNotifyActorResumedFromCheckpoint(
    JNIEnv *env, jclass, jbyteArray checkpointId) {
  const auto &actor_id =
      ray::CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentActorID();
  const auto checkpoint_id = JavaByteArrayToId<ActorCheckpointID>(env, checkpointId);
  auto status = ray::CoreWorkerProcess::GetCoreWorker().NotifyActorResumedFromCheckpoint(
      actor_id, checkpoint_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

#ifdef __cplusplus
}
#endif
