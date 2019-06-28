#include "ray/core_worker/lib/java/org_ray_runtime_WorkerContext.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_helper.h"

inline ray::WorkerContext &GetContext(jlong nativeCoreWorker) {
  return reinterpret_cast<ray::CoreWorker *>(nativeCoreWorker)->Context();
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentJobId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentJobId(
    JNIEnv *env, jclass o, jlong nativeCoreWorker) {
  return JByteArrayFromUniqueId<ray::JobID>(
             env, GetContext(nativeCoreWorker).GetCurrentJobID())
      .GetJByteArray();
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentWorkerId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentWorkerId(
    JNIEnv *env, jclass o, jlong nativeCoreWorker) {
  return JByteArrayFromUniqueId<ray::WorkerID>(env,
                                               GetContext(nativeCoreWorker).GetWorkerID())
      .GetJByteArray();
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentActorId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentActorId(
    JNIEnv *env, jclass o, jlong nativeCoreWorker) {
  return JByteArrayFromUniqueId<ray::ActorID>(
             env, GetContext(nativeCoreWorker).GetCurrentActorID())
      .GetJByteArray();
}

#ifdef __cplusplus
}
#endif
