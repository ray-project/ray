#include "ray/core_worker/lib/java/org_ray_runtime_WorkerContextImpl.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"

inline ray::WorkerContext &GetWorkerContextFromPointer(jlong nativeCoreWorkerPointer) {
  return reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)->Context();
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_WorkerContextImpl
 * Method:    nativeGetCurrentTask
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContextImpl_nativeGetCurrentTask(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  auto spec = GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetCurrentTask();
  if (!spec) {
    return nullptr;
  }

  auto task_message = spec->Serialize();
  jbyteArray result = env->NewByteArray(task_message.size());
  env->SetByteArrayRegion(
      result, 0, task_message.size(),
      reinterpret_cast<jbyte *>(const_cast<char *>(task_message.data())));
  return result;
}

/*
 * Class:     org_ray_runtime_WorkerContextImpl
 * Method:    nativeGetCurrentJobId
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_WorkerContextImpl_nativeGetCurrentJobId(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  const auto &job_id =
      GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetCurrentJobID();
  return IdToJavaByteBuffer<ray::JobID>(env, job_id);
}

/*
 * Class:     org_ray_runtime_WorkerContextImpl
 * Method:    nativeGetCurrentWorkerId
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_WorkerContextImpl_nativeGetCurrentWorkerId(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  const auto &worker_id =
      GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetWorkerID();
  return IdToJavaByteBuffer<ray::WorkerID>(env, worker_id);
}

/*
 * Class:     org_ray_runtime_WorkerContextImpl
 * Method:    nativeGetCurrentActorId
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_WorkerContextImpl_nativeGetCurrentActorId(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  const auto &actor_id =
      GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetCurrentActorID();
  return IdToJavaByteBuffer<ray::ActorID>(env, actor_id);
}

#ifdef __cplusplus
}
#endif
