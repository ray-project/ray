#include "ray/core_worker/lib/java/org_ray_runtime_RayActorImpl.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/lib/java/jni_helper.h"
#include "ray/core_worker/task_interface.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_RayActorImpl
 * Method:    fork
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_RayActorImpl_fork(JNIEnv *env, jclass o,
                                                               jlong nativeActorHandle) {
  auto &actor_handle = *(reinterpret_cast<ray::ActorHandle *>(nativeActorHandle));
  auto new_actor_handle = actor_handle.Fork();
  return reinterpret_cast<jlong>(new_actor_handle.release());
}

#ifdef __cplusplus
}
#endif
