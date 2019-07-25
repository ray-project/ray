#include "ray/core_worker/lib/java/org_ray_runtime_objectstore_ObjectInterfaceImpl.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/lib/java/jni_utils.h"
#include "ray/core_worker/object_interface.h"

using ray::rpc::RayletClient;

inline ray::CoreWorkerObjectInterface *GetObjectInterfaceFromPointer(
    jlong nativeObjectInterfacePointer) {
  return reinterpret_cast<ray::CoreWorkerObjectInterface *>(nativeObjectInterfacePointer);
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterfaceImpl
 * Method:    nativeCreateObjectInterface
 * Signature: (JJLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL
Java_org_ray_runtime_objectstore_ObjectInterfaceImpl_nativeCreateObjectInterface(
    JNIEnv *env, jclass, jlong nativeWorkerContext, jlong nativeRayletClient,
    jstring storeSocketName) {
  return reinterpret_cast<jlong>(new ray::CoreWorkerObjectInterface(
      *reinterpret_cast<ray::WorkerContext *>(nativeWorkerContext),
      *reinterpret_cast<std::unique_ptr<RayletClient> *>(nativeRayletClient),
      JavaStringToNativeString(env, storeSocketName)));
}

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterfaceImpl
 * Method:    nativePut
 * Signature: (JLorg/ray/runtime/objectstore/NativeRayObject;)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_objectstore_ObjectInterfaceImpl_nativePut__JLorg_ray_runtime_objectstore_NativeRayObject_2(
    JNIEnv *env, jclass, jlong nativeObjectInterfacePointer, jobject obj) {
  ray::Status status;
  ray::ObjectID object_id = ReadJavaNativeRayObject<ray::ObjectID>(
      env, obj,
      [nativeObjectInterfacePointer,
       &status](const std::shared_ptr<ray::RayObject> &rayObject) {
        RAY_CHECK(rayObject != nullptr);
        ray::ObjectID object_id;
        status = GetObjectInterfaceFromPointer(nativeObjectInterfacePointer)
                     ->Put(*rayObject, &object_id);
        return object_id;
      });
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return IdToJavaByteArray<ray::ObjectID>(env, object_id);
}

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterfaceImpl
 * Method:    nativePut
 * Signature: (J[BLorg/ray/runtime/objectstore/NativeRayObject;)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_objectstore_ObjectInterfaceImpl_nativePut__J_3BLorg_ray_runtime_objectstore_NativeRayObject_2(
    JNIEnv *env, jclass, jlong nativeObjectInterfacePointer, jbyteArray objectId,
    jobject obj) {
  auto object_id = JavaByteArrayToId<ray::ObjectID>(env, objectId);
  auto status = ReadJavaNativeRayObject<ray::Status>(
      env, obj,
      [nativeObjectInterfacePointer,
       &object_id](const std::shared_ptr<ray::RayObject> &rayObject) {
        RAY_CHECK(rayObject != nullptr);
        return GetObjectInterfaceFromPointer(nativeObjectInterfacePointer)
            ->Put(*rayObject, object_id);
      });
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterfaceImpl
 * Method:    nativeGet
 * Signature: (JLjava/util/List;J)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_objectstore_ObjectInterfaceImpl_nativeGet(
    JNIEnv *env, jclass, jlong nativeObjectInterfacePointer, jobject ids,
    jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, ids, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  std::vector<std::shared_ptr<ray::RayObject>> results;
  auto status = GetObjectInterfaceFromPointer(nativeObjectInterfacePointer)
                    ->Get(object_ids, (int64_t)timeoutMs, &results);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeVectorToJavaList<std::shared_ptr<ray::RayObject>>(env, results,
                                                                 ToJavaNativeRayObject);
}

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterfaceImpl
 * Method:    nativeWait
 * Signature: (JLjava/util/List;IJ)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_objectstore_ObjectInterfaceImpl_nativeWait(
    JNIEnv *env, jclass, jlong nativeObjectInterfacePointer, jobject objectIds,
    jint numObjects, jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  std::vector<bool> results;
  auto status = GetObjectInterfaceFromPointer(nativeObjectInterfacePointer)
                    ->Wait(object_ids, (int)numObjects, (int64_t)timeoutMs, &results);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeVectorToJavaList<bool>(env, results, [](JNIEnv *env, const bool &item) {
    return env->NewObject(java_boolean_class, java_boolean_init, (jboolean)item);
  });
}

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterfaceImpl
 * Method:    nativeDelete
 * Signature: (JLjava/util/List;ZZ)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_objectstore_ObjectInterfaceImpl_nativeDelete(
    JNIEnv *env, jclass, jlong nativeObjectInterfacePointer, jobject objectIds,
    jboolean localOnly, jboolean deleteCreatingTasks) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  auto status = GetObjectInterfaceFromPointer(nativeObjectInterfacePointer)
                    ->Delete(object_ids, (bool)localOnly, (bool)deleteCreatingTasks);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

/*
 * Class:     org_ray_runtime_objectstore_ObjectInterfaceImpl
 * Method:    nativeDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_objectstore_ObjectInterfaceImpl_nativeDestroy(
    JNIEnv *env, jclass, jlong nativeObjectInterfacePointer) {
  delete GetObjectInterfaceFromPointer(nativeObjectInterfacePointer);
}

#ifdef __cplusplus
}
#endif
