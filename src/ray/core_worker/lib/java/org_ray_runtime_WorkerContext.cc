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
 * Method:    getCurrentJobId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_getCurrentJobId(
    JNIEnv *env, jclass o, jlong nativeCoreWorker) {
  return JByteArrayFromUniqueId<ray::JobID>(
             env, GetContext(nativeCoreWorker).GetCurrentJobID())
      .GetJByteArray();
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    getCurrentWorkerId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_getCurrentWorkerId(
    JNIEnv *env, jclass o, jlong nativeCoreWorker) {
  return JByteArrayFromUniqueId<ray::ClientID>(env,
                                               GetContext(nativeCoreWorker).GetWorkerID())
      .GetJByteArray();
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    getCurrentActorId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_getCurrentActorId(
    JNIEnv *env, jclass o, jlong nativeCoreWorker) {
  return JByteArrayFromUniqueId<ray::ActorID>(
             env, GetContext(nativeCoreWorker).GetCurrentActorID())
      .GetJByteArray();
}

#ifdef __cplusplus
}
#endif
