#include "ray/core_worker/lib/java/org_ray_runtime_WorkerContext.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/lib/java/jni_utils.h"

inline ray::WorkerContext *GetWorkerContextFromPointer(
    jlong nativeWorkerContextFromPointer) {
  return reinterpret_cast<ray::WorkerContext *>(nativeWorkerContextFromPointer);
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeCreateWorkerContext
 * Signature: (I[B)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_WorkerContext_nativeCreateWorkerContext(
    JNIEnv *env, jclass, jint workerType, jbyteArray jobId) {
  return reinterpret_cast<jlong>(
      new ray::WorkerContext(static_cast<ray::rpc::WorkerType>(workerType),
                             JavaByteArrayToId<ray::JobID>(env, jobId)));
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentTaskId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentTaskId(
    JNIEnv *env, jclass, jlong nativeWorkerContextFromPointer) {
  auto task_id =
      GetWorkerContextFromPointer(nativeWorkerContextFromPointer)->GetCurrentTaskID();
  return IdToJavaByteArray<ray::TaskID>(env, task_id);
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeSetCurrentTask
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_WorkerContext_nativeSetCurrentTask(
    JNIEnv *env, jclass, jlong nativeWorkerContextFromPointer, jbyteArray taskSpec) {
  jbyte *data = env->GetByteArrayElements(taskSpec, NULL);
  jsize size = env->GetArrayLength(taskSpec);
  ray::rpc::TaskSpec task_spec_message;
  task_spec_message.ParseFromArray(data, size);
  env->ReleaseByteArrayElements(taskSpec, data, JNI_ABORT);

  ray::TaskSpecification spec(task_spec_message);
  GetWorkerContextFromPointer(nativeWorkerContextFromPointer)->SetCurrentTask(spec);
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentTask
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentTask(
    JNIEnv *env, jclass, jlong nativeWorkerContextFromPointer) {
  auto spec =
      GetWorkerContextFromPointer(nativeWorkerContextFromPointer)->GetCurrentTask();
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
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentJobId
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentJobId(
    JNIEnv *env, jclass, jlong nativeWorkerContextFromPointer) {
  const auto &job_id =
      GetWorkerContextFromPointer(nativeWorkerContextFromPointer)->GetCurrentJobID();
  return IdToJavaByteBuffer<ray::JobID>(env, job_id);
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetCurrentWorkerId
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_WorkerContext_nativeGetCurrentWorkerId(
    JNIEnv *env, jclass, jlong nativeWorkerContextFromPointer) {
  auto worker_id =
      GetWorkerContextFromPointer(nativeWorkerContextFromPointer)->GetWorkerID();
  return IdToJavaByteArray<ray::WorkerID>(env, worker_id);
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetNextTaskIndex
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_ray_runtime_WorkerContext_nativeGetNextTaskIndex(
    JNIEnv *env, jclass, jlong nativeWorkerContextFromPointer) {
  return GetWorkerContextFromPointer(nativeWorkerContextFromPointer)->GetNextTaskIndex();
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeGetNextPutIndex
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_ray_runtime_WorkerContext_nativeGetNextPutIndex(
    JNIEnv *env, jclass, jlong nativeWorkerContextFromPointer) {
  return GetWorkerContextFromPointer(nativeWorkerContextFromPointer)->GetNextPutIndex();
}

/*
 * Class:     org_ray_runtime_WorkerContext
 * Method:    nativeDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_WorkerContext_nativeDestroy(
    JNIEnv *env, jclass, jlong nativeWorkerContextFromPointer) {
  delete GetWorkerContextFromPointer(nativeWorkerContextFromPointer);
}

#ifdef __cplusplus
}
#endif
