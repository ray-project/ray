#include "ray/core_worker/lib/java/org_ray_runtime_ObjectInterface.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_helper.h"
#include "ray/core_worker/object_interface.h"

inline ray::CoreWorkerObjectInterface &GetObjectInterface(jlong nativeCoreWorker) {
  return reinterpret_cast<ray::CoreWorker *>(nativeCoreWorker)->Objects();
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    nativePut
 * Signature: (JLorg/ray/runtime/proxyTypes/RayObjectProxy;)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_ObjectInterface_nativePut__JLorg_ray_runtime_proxyTypes_RayObjectProxy_2(
    JNIEnv *env, jclass o, jlong nativeCoreWorker, jobject rayObjectProxy) {
  ray::Status status;
  ray::ObjectID object_id = ReadJavaRayObjectProxy<ray::ObjectID>(
      env, rayObjectProxy,
      [nativeCoreWorker, &status](const std::shared_ptr<ray::RayObject> &rayObject) {
        RAY_CHECK(rayObject != nullptr);
        ray::ObjectID object_id;
        status = GetObjectInterface(nativeCoreWorker).Put(*rayObject, &object_id);
        return object_id;
      });
  ThrowRayExceptionIfNotOK(env, status);
  return JByteArrayFromUniqueId<ray::ObjectID>(env, object_id).GetJByteArray();
}

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    nativePut
 * Signature: (J[BLorg/ray/runtime/proxyTypes/RayObjectProxy;)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_ObjectInterface_nativePut__J_3BLorg_ray_runtime_proxyTypes_RayObjectProxy_2(
    JNIEnv *env, jclass o, jlong nativeCoreWorker, jbyteArray objectId,
    jobject rayObjectProxy) {
  auto object_id = UniqueIdFromJByteArray<ray::ObjectID>(env, objectId).GetId();
  auto status = ReadJavaRayObjectProxy<ray::Status>(
      env, rayObjectProxy,
      [nativeCoreWorker, &object_id](const std::shared_ptr<ray::RayObject> &rayObject) {
        RAY_CHECK(rayObject != nullptr);
        return GetObjectInterface(nativeCoreWorker).Put(*rayObject, object_id);
      });
  ThrowRayExceptionIfNotOK(env, status);
}

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    nativeGet
 * Signature: (JLjava/util/List;J)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_ObjectInterface_nativeGet(
    JNIEnv *env, jclass o, jlong nativeCoreWorker, jobject ids, jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, ids, &object_ids, [](JNIEnv *env, jobject id) {
        return UniqueIdFromJByteArray<ray::ObjectID>(env, static_cast<jbyteArray>(id))
            .GetId();
      });
  std::vector<std::shared_ptr<ray::RayObject>> results;
  auto status =
      GetObjectInterface(nativeCoreWorker).Get(object_ids, (int64_t)timeoutMs, &results);
  ThrowRayExceptionIfNotOK(env, status);
  return NativeVectorToJavaList<std::shared_ptr<ray::RayObject>>(env, results,
                                                                 ToJavaRayObjectProxy);
}

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    nativeWait
 * Signature: (JLjava/util/List;IJ)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_ObjectInterface_nativeWait(
    JNIEnv *env, jclass o, jlong nativeCoreWorker, jobject objectIds, jint numObjects,
    jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return UniqueIdFromJByteArray<ray::ObjectID>(env, static_cast<jbyteArray>(id))
            .GetId();
      });
  std::vector<bool> results;
  auto status = GetObjectInterface(nativeCoreWorker)
                    .Wait(object_ids, (int)numObjects, (int64_t)timeoutMs, &results);
  ThrowRayExceptionIfNotOK(env, status);
  return NativeVectorToJavaList<bool>(env, results, [](JNIEnv *env, const bool &item) {
    return env->NewObject(java_boolean_class, java_boolean_init, (jboolean)item);
  });
}

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    nativeDelete
 * Signature: (JLjava/util/List;ZZ)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_ObjectInterface_nativeDelete(
    JNIEnv *env, jclass o, jlong nativeCoreWorker, jobject objectIds, jboolean localOnly,
    jboolean deleteCreatingTasks) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return UniqueIdFromJByteArray<ray::ObjectID>(env, static_cast<jbyteArray>(id))
            .GetId();
      });
  auto status = GetObjectInterface(nativeCoreWorker)
                    .Delete(object_ids, (bool)localOnly, (bool)deleteCreatingTasks);
  ThrowRayExceptionIfNotOK(env, status);
}

#ifdef __cplusplus
}
#endif
