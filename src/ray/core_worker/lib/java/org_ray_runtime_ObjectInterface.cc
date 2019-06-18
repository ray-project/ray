#include "ray/core_worker/lib/java/org_ray_runtime_ObjectInterface.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/lib/java/jni_helper.h"
#include "ray/core_worker/object_interface.h"

template <typename ReturnT>
inline ReturnT ReadBinary(JNIEnv *env, const jbyteArray &binary,
                          std::function<ReturnT(const ray::Buffer &)> reader) {
  auto data_size = env->GetArrayLength(binary);
  jbyte *data = env->GetByteArrayElements(binary, nullptr);
  ray::LocalMemoryBuffer buffer(reinterpret_cast<uint8_t *>(data), data_size);
  auto result = reader(buffer);
  env->ReleaseByteArrayElements(binary, data, JNI_ABORT);
  return result;
}

inline ray::CoreWorkerObjectInterface &GetObjectInterface(jlong nativeCoreWorker) {
  return reinterpret_cast<ray::CoreWorker *>(nativeCoreWorker)->Objects();
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    put
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_ObjectInterface_put__J_3B(
    JNIEnv *env, jclass o, jlong nativeCoreWorker, jbyteArray binary) {
  ray::Status status;
  ray::ObjectID object_id = ReadBinary<ray::ObjectID>(
      env, binary, [nativeCoreWorker, &status](const ray::Buffer &buffer) {
        ray::ObjectID object_id;
        status = GetObjectInterface(nativeCoreWorker).Put(buffer, &object_id);
        return object_id;
      });
  ThrowRayExceptionIfNotOK(env, status);
  return JByteArrayFromUniqueId<ray::ObjectID>(env, object_id).GetJByteArray();
}

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    put
 * Signature: (J[B[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_ObjectInterface_put__J_3B_3B(
    JNIEnv *env, jclass o, jlong nativeCoreWorker, jbyteArray objectId,
    jbyteArray binary) {
  auto object_id = UniqueIdFromJByteArray<ray::ObjectID>(env, objectId).GetId();
  auto status = ReadBinary<ray::Status>(
      env, binary, [nativeCoreWorker, &object_id](const ray::Buffer &buffer) {
        return GetObjectInterface(nativeCoreWorker).Put(buffer, object_id);
      });
  ThrowRayExceptionIfNotOK(env, status);
}

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    get
 * Signature: (JLjava/util/List;J)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_ObjectInterface_get(JNIEnv *env, jclass o,
                                                                   jlong nativeCoreWorker,
                                                                   jobject ids,
                                                                   jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, ids, &object_ids, [](JNIEnv *env, jobject id) {
        return UniqueIdFromJByteArray<ray::ObjectID>(env, static_cast<jbyteArray>(id))
            .GetId();
      });
  std::vector<std::shared_ptr<ray::Buffer>> results;
  auto status =
      GetObjectInterface(nativeCoreWorker).Get(object_ids, (int64_t)timeoutMs, &results);
  ThrowRayExceptionIfNotOK(env, status);
  return NativeBufferVectorToJavaBinaryList(env, results);
}

/*
 * Class:     org_ray_runtime_ObjectInterface
 * Method:    wait
 * Signature: (JLjava/util/List;IJ)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_org_ray_runtime_ObjectInterface_wait(
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
 * Method:    delete
 * Signature: (JLjava/util/List;ZZ)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_ObjectInterface_delete(
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
