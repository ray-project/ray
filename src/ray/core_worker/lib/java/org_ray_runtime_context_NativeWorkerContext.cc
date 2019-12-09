#include "ray/core_worker/lib/java/org_ray_runtime_context_NativeWorkerContext.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"

inline ray::WorkerContext &GetWorkerContextFromPointer(jlong nativeCoreWorkerPointer) {
  return reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)->GetWorkerContext();
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jint JNICALL
Java_org_ray_runtime_context_NativeWorkerContext_nativeGetCurrentTaskType(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  auto task_spec = GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetCurrentTask();
  RAY_CHECK(task_spec) << "Current task is not set.";
  return static_cast<int>(task_spec->GetMessage().type());
}

JNIEXPORT jobject JNICALL
Java_org_ray_runtime_context_NativeWorkerContext_nativeGetCurrentTaskId(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  const ray::TaskID &task_id =
      GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetCurrentTaskID();
  return IdToJavaByteBuffer<ray::TaskID>(env, task_id);
}

JNIEXPORT jobject JNICALL
Java_org_ray_runtime_context_NativeWorkerContext_nativeGetCurrentJobId(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  const auto &job_id =
      GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetCurrentJobID();
  return IdToJavaByteBuffer<ray::JobID>(env, job_id);
}

JNIEXPORT jobject JNICALL
Java_org_ray_runtime_context_NativeWorkerContext_nativeGetCurrentWorkerId(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  const auto &worker_id =
      GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetWorkerID();
  return IdToJavaByteBuffer<ray::WorkerID>(env, worker_id);
}

JNIEXPORT jobject JNICALL
Java_org_ray_runtime_context_NativeWorkerContext_nativeGetCurrentActorId(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  const auto &actor_id =
      GetWorkerContextFromPointer(nativeCoreWorkerPointer).GetCurrentActorID();
  return IdToJavaByteBuffer<ray::ActorID>(env, actor_id);
}

#ifdef __cplusplus
}
#endif
