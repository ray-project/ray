// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "io_ray_runtime_object_NativeObjectStore.h"
#include <jni.h>
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "jni_utils.h"

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_object_NativeObjectStore_nativePut__Lio_ray_runtime_object_NativeRayObject_2(
    JNIEnv *env, jclass, jobject obj) {
  auto ray_object = JavaNativeRayObjectToNativeRayObject(env, obj);
  RAY_CHECK(ray_object != nullptr);
  ray::ObjectID object_id;
  auto status = ray::CoreWorkerProcess::GetCoreWorker().Put(*ray_object, {}, &object_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return IdToJavaByteArray<ray::ObjectID>(env, object_id);
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_object_NativeObjectStore_nativePut___3BLio_ray_runtime_object_NativeRayObject_2(
    JNIEnv *env, jclass, jbyteArray objectId, jobject obj) {
  auto object_id = JavaByteArrayToId<ray::ObjectID>(env, objectId);
  auto ray_object = JavaNativeRayObjectToNativeRayObject(env, obj);
  RAY_CHECK(ray_object != nullptr);
  auto status = ray::CoreWorkerProcess::GetCoreWorker().Put(*ray_object, {}, object_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT jobject JNICALL Java_io_ray_runtime_object_NativeObjectStore_nativeGet(
    JNIEnv *env, jclass, jobject ids, jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, ids, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  std::vector<std::shared_ptr<ray::RayObject>> results;
  auto status = ray::CoreWorkerProcess::GetCoreWorker().Get(object_ids,
                                                            (int64_t)timeoutMs, &results);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeVectorToJavaList<std::shared_ptr<ray::RayObject>>(
      env, results, NativeRayObjectToJavaNativeRayObject);
}

JNIEXPORT jobject JNICALL Java_io_ray_runtime_object_NativeObjectStore_nativeWait(
    JNIEnv *env, jclass, jobject objectIds, jint numObjects, jlong timeoutMs) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  std::vector<bool> results;
  auto status = ray::CoreWorkerProcess::GetCoreWorker().Wait(
      object_ids, (int)numObjects, (int64_t)timeoutMs, &results);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeVectorToJavaList<bool>(env, results, [](JNIEnv *env, const bool &item) {
    return env->NewObject(java_boolean_class, java_boolean_init, (jboolean)item);
  });
}

JNIEXPORT void JNICALL Java_io_ray_runtime_object_NativeObjectStore_nativeDelete(
    JNIEnv *env, jclass, jobject objectIds, jboolean localOnly,
    jboolean deleteCreatingTasks) {
  std::vector<ray::ObjectID> object_ids;
  JavaListToNativeVector<ray::ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ray::ObjectID>(env, static_cast<jbyteArray>(id));
      });
  auto status = ray::CoreWorkerProcess::GetCoreWorker().Delete(
      object_ids, (bool)localOnly, (bool)deleteCreatingTasks);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

#ifdef __cplusplus
}
#endif
