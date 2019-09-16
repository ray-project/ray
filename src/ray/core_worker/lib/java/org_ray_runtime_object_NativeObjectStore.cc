#include "ray/core_worker/lib/java/org_ray_runtime_object_NativeObjectStore.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"
#include "ray/core_worker/object_interface.h"

inline ray::CoreWorkerObjectInterface &GetObjectInterfaceFromPointer(
    jlong nativeCoreWorkerPointer) {
  return reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)->Objects();
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_object_NativeObjectStore
 * Method:    nativePut
 * Signature: (JLorg/ray/runtime/object/NativeRayObject;)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_object_NativeObjectStore_nativePut__JLorg_ray_runtime_object_NativeRayObject_2(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jobject obj) {
  auto ray_object = JavaNativeRayObjectToNativeRayObject(env, obj);
  RAY_CHECK(ray_object != nullptr);
  ray::ObjectID object_id;
  auto status =
      GetObjectInterfaceFromPointer(nativeCoreWorkerPointer).Put(*ray_object, &object_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return IdToJavaByteArray<ray::ObjectID>(env, object_id);
}

/*
 * Class:     org_ray_runtime_object_NativeObjectStore
 * Method:    nativePut
 * Signature: (J[BLorg/ray/runtime/object/NativeRayObject;)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_object_NativeObjectStore_nativePut__J_3BLorg_ray_runtime_object_NativeRayObject_2(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jbyteArray objectId,
    jobject obj) {
  auto object_id = JavaByteArrayToId<ray::ObjectID>(env, objectId);
  auto ray_object = JavaNativeRayObjectToNativeRayObject(env, obj);
  RAY_CHECK(ray_object != nullptr);
  auto status =
      GetObjectInterfaceFromPointer(nativeCoreWorkerPointer).Put(*ray_object, object_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

/*
 * Class:     org_ray_runtime_object_NativeObjectStore
 * Method:    nativeGet
 * Signature: (JLjava/util/List;J)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_object_NativeObjectStore_nativeGet(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jobject ids, jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, ids, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  std::vector<std::shared_ptr<ray::RayObject>> results;
  auto status = GetObjectInterfaceFromPointer(nativeCoreWorkerPointer)
                    .Get(object_ids, (int64_t)timeoutMs, &results);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeVectorToJavaList<std::shared_ptr<ray::RayObject>>(
      env, results, NativeRayObjectToJavaNativeRayObject);
}

/*
 * Class:     org_ray_runtime_object_NativeObjectStore
 * Method:    nativeWait
 * Signature: (JLjava/util/List;IJ)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_object_NativeObjectStore_nativeWait(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jobject objectIds,
    jint numObjects, jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  std::vector<bool> results;
  auto status = GetObjectInterfaceFromPointer(nativeCoreWorkerPointer)
                    .Wait(object_ids, (int)numObjects, (int64_t)timeoutMs, &results);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeVectorToJavaList<bool>(env, results, [](JNIEnv *env, const bool &item) {
    return env->NewObject(java_boolean_class, java_boolean_init, (jboolean)item);
  });
}

/*
 * Class:     org_ray_runtime_object_NativeObjectStore
 * Method:    nativeDelete
 * Signature: (JLjava/util/List;ZZ)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_object_NativeObjectStore_nativeDelete(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jobject objectIds,
    jboolean localOnly, jboolean deleteCreatingTasks) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  auto status = GetObjectInterfaceFromPointer(nativeCoreWorkerPointer)
                    .Delete(object_ids, (bool)localOnly, (bool)deleteCreatingTasks);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

#ifdef __cplusplus
}
#endif
