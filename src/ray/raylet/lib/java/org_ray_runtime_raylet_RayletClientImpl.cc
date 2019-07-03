#include "ray/raylet/lib/java/org_ray_runtime_raylet_RayletClientImpl.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_helper.h"
#include "ray/raylet/raylet_client.h"

inline RayletClient &GetRayletClient(jlong conn) {
  return **reinterpret_cast<std::shared_ptr<RayletClient> *>(conn);
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeInit
 * Signature: (Ljava/lang/String;[BZ[B)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeInit(
    JNIEnv *env, jclass, jstring sockName, jbyteArray workerId, jboolean isWorker,
    jbyteArray jobId) {
  UniqueIdFromJByteArray<ClientID> worker_id(env, workerId);
  UniqueIdFromJByteArray<JobID> job_id(env, jobId);
  const char *nativeString = env->GetStringUTFChars(sockName, JNI_FALSE);
  auto raylet_client = new std::shared_ptr<RayletClient>();
  *raylet_client = std::make_shared<RayletClient>(
      nativeString, worker_id.GetId(), isWorker, job_id.GetId(), Language::JAVA);
  env->ReleaseStringUTFChars(sockName, nativeString);
  return reinterpret_cast<jlong>(raylet_client);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativePrepareCheckpoint
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativePrepareCheckpoint(JNIEnv *env, jclass,
                                                                     jlong conn,
                                                                     jbyteArray actorId) {
  UniqueIdFromJByteArray<ActorID> actor_id(env, actorId);
  ActorCheckpointID checkpoint_id;
  auto status =
      GetRayletClient(conn).PrepareActorCheckpoint(actor_id.GetId(), checkpoint_id);
  if (ThrowRayExceptionIfNotOK(env, status)) {
    return nullptr;
  }
  jbyteArray result = env->NewByteArray(checkpoint_id.Size());
  env->SetByteArrayRegion(result, 0, checkpoint_id.Size(),
                          reinterpret_cast<const jbyte *>(checkpoint_id.Data()));
  return result;
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeNotifyActorResumedFromCheckpoint
 * Signature: (J[B[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeNotifyActorResumedFromCheckpoint(
    JNIEnv *env, jclass, jlong conn, jbyteArray actorId, jbyteArray checkpointId) {
  UniqueIdFromJByteArray<ActorID> actor_id(env, actorId);
  UniqueIdFromJByteArray<ActorCheckpointID> checkpoint_id(env, checkpointId);
  auto status = GetRayletClient(conn).NotifyActorResumedFromCheckpoint(
      actor_id.GetId(), checkpoint_id.GetId());
  ThrowRayExceptionIfNotOK(env, status);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeSetResource
 * Signature: (JLjava/lang/String;D[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeSetResource(
    JNIEnv *env, jclass, jlong conn, jstring resourceName, jdouble capacity,
    jbyteArray nodeId) {
  UniqueIdFromJByteArray<ClientID> node_id(env, nodeId);
  const char *native_resource_name = env->GetStringUTFChars(resourceName, JNI_FALSE);

  auto status = GetRayletClient(conn).SetResource(
      native_resource_name, static_cast<double>(capacity), node_id.GetId());
  env->ReleaseStringUTFChars(resourceName, native_resource_name);
  ThrowRayExceptionIfNotOK(env, status);
}

#ifdef __cplusplus
}
#endif
