#include "ray/core_worker/lib/java/org_ray_runtime_task_NativeTaskExecutor.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"
#include "ray/raylet/raylet_client.h"

#ifdef __cplusplus
extern "C" {
#endif

using ray::ClientID;

/*
 * Class:     org_ray_runtime_task_NativeTaskExecutor
 * Method:    nativePrepareCheckpoint
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_task_NativeTaskExecutor_nativePrepareCheckpoint(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  auto &core_worker = *reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
  const auto &actor_id = core_worker.GetWorkerContext().GetCurrentActorID();
  const auto &task_spec = core_worker.GetWorkerContext().GetCurrentTask();
  RAY_CHECK(task_spec->IsActorTask());
  ActorCheckpointID checkpoint_id;
  auto status = core_worker.GetRayletClient().PrepareActorCheckpoint(
      actor_id, checkpoint_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  jbyteArray result = env->NewByteArray(checkpoint_id.Size());
  env->SetByteArrayRegion(result, 0, checkpoint_id.Size(),
                          reinterpret_cast<const jbyte *>(checkpoint_id.Data()));
  return result;
}

/*
 * Class:     org_ray_runtime_task_NativeTaskExecutor
 * Method:    nativeNotifyActorResumedFromCheckpoint
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_task_NativeTaskExecutor_nativeNotifyActorResumedFromCheckpoint(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jbyteArray checkpointId) {
  auto &core_worker = *reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer);
  const auto &actor_id = core_worker.GetWorkerContext().GetCurrentActorID();
  const auto checkpoint_id = JavaByteArrayToId<ActorCheckpointID>(env, checkpointId);
  auto status = core_worker.GetRayletClient().NotifyActorResumedFromCheckpoint(
      actor_id, checkpoint_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

#ifdef __cplusplus
}
#endif
