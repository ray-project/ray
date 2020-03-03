#include "ray/core_worker/lib/java/org_ray_runtime_object_NativeObjectStore.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_utils.h"

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_object_NativeObjectStore_nativePut__JLorg_ray_runtime_object_NativeRayObject_2Ljava_util_List_2(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jobject obj, jobject innerIds) {
  auto ray_object = JavaNativeRayObjectToNativeRayObject(env, obj, innerIds);
  RAY_CHECK(ray_object != nullptr);
  ray::ObjectID object_id;
  auto status = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)
                    ->Put(*ray_object, {}, &object_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return IdToJavaByteArray<ray::ObjectID>(env, object_id);
}

JNIEXPORT void JNICALL
Java_org_ray_runtime_object_NativeObjectStore_nativePut__J_3BLorg_ray_runtime_object_NativeRayObject_2Ljava_util_List_2(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jbyteArray objectId,
    jobject obj, jobject innerIds) {
  auto object_id = JavaByteArrayToId<ray::ObjectID>(env, objectId);
  auto ray_object = JavaNativeRayObjectToNativeRayObject(env, obj, innerIds);
  RAY_CHECK(ray_object != nullptr);
  auto status = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)
                    ->Put(*ray_object, {}, object_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT jobject JNICALL Java_org_ray_runtime_object_NativeObjectStore_nativeGet(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jobject ids, jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, ids, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  std::vector<std::shared_ptr<ray::RayObject>> results;
  auto status = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)
                    ->Get(object_ids, (int64_t)timeoutMs, &results);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeVectorToJavaList<std::shared_ptr<ray::RayObject>>(
      env, results, NativeRayObjectToJavaNativeRayObject);
}

JNIEXPORT jobject JNICALL Java_org_ray_runtime_object_NativeObjectStore_nativeWait(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jobject objectIds,
    jint numObjects, jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  std::vector<bool> results;
  auto status = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)
                    ->Wait(object_ids, (int)numObjects, (int64_t)timeoutMs, &results);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeVectorToJavaList<bool>(env, results, [](JNIEnv *env, const bool &item) {
    jobject java_item =
        env->NewObject(java_boolean_class, java_boolean_init, (jboolean)item);
    RAY_CHECK_JAVA_EXCEPTION(env);
    return java_item;
  });
}

JNIEXPORT void JNICALL Java_org_ray_runtime_object_NativeObjectStore_nativeDelete(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jobject objectIds,
    jboolean localOnly, jboolean deleteCreatingTasks) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  auto status = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)
                    ->Delete(object_ids, (bool)localOnly, (bool)deleteCreatingTasks);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT void JNICALL
Java_org_ray_runtime_object_NativeObjectStore_nativeAddLocalReference(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jbyteArray objectId) {
  auto object_id = JavaByteArrayToId<ray::ObjectID>(env, objectId);
  reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)
      ->AddLocalReference(object_id);
}

JNIEXPORT void JNICALL
Java_org_ray_runtime_object_NativeObjectStore_nativeRemoveLocalReference(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer, jbyteArray objectId) {
  auto object_id = JavaByteArrayToId<ray::ObjectID>(env, objectId);
  reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)
      ->RemoveLocalReference(object_id);
}

JNIEXPORT jobject JNICALL
Java_org_ray_runtime_object_NativeObjectStore_nativeGetAllReferenceCounts(
    JNIEnv *env, jclass, jlong nativeCoreWorkerPointer) {
  auto reference_counts = reinterpret_cast<ray::CoreWorker *>(nativeCoreWorkerPointer)
                              ->GetAllReferenceCounts();
  return NativeMapToJavaMap<ray::ObjectID, std::pair<size_t, size_t>>(
      env, reference_counts,
      [](JNIEnv *env, const ray::ObjectID &key) {
        return IdToJavaByteArray<ObjectID>(env, key);
      },
      [](JNIEnv *env, const std::pair<size_t, size_t> &value) {
        jlongArray array = env->NewLongArray(2);
        jlong *elements = env->GetLongArrayElements(array, nullptr);
        elements[0] = static_cast<jlong>(value.first);
        elements[1] = static_cast<jlong>(value.second);
        env->ReleaseLongArrayElements(array, elements, 0);
        return array;
      });
}

#ifdef __cplusplus
}
#endif
