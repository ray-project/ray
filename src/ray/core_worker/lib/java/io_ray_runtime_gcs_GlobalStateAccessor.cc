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

#include "io_ray_runtime_gcs_GlobalStateAccessor.h"

#include <jni.h>

#include "jni_utils.h"
#include "ray/core_worker/common.h"
#include "ray/gcs/gcs_client/global_state_accessor.h"

#ifdef __cplusplus
extern "C" {
#endif
JNIEXPORT jlong JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeCreateGlobalStateAccessor(
    JNIEnv *env, jobject o, jstring j_redis_address, jstring j_redis_passowrd) {
  std::string redis_address = JavaStringToNativeString(env, j_redis_address);
  std::string redis_password = JavaStringToNativeString(env, j_redis_passowrd);
  ray::gcs::GlobalStateAccessor *gcs_accessor =
      new ray::gcs::GlobalStateAccessor(redis_address, redis_password);
  return reinterpret_cast<jlong>(gcs_accessor);
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeDestroyGlobalStateAccessor(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  delete gcs_accessor;
}

JNIEXPORT jboolean JNICALL Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeConnect(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  return gcs_accessor->Connect();
}

JNIEXPORT jobject JNICALL Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetAllJobInfo(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto job_info_list = gcs_accessor->GetAllJobInfo();
  return NativeVectorToJavaList<std::string>(
      env, job_info_list, [](JNIEnv *env, const std::string &str) {
        return NativeStringToJavaByteArray(env, str);
      });
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetAllNodeInfo(JNIEnv *env, jobject o,
                                                                 jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto node_info_list = gcs_accessor->GetAllNodeInfo();
  return NativeVectorToJavaList<std::string>(
      env, node_info_list, [](JNIEnv *env, const std::string &str) {
        return NativeStringToJavaByteArray(env, str);
      });
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetNodeResourceInfo(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr, jbyteArray node_id_bytes) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto node_id = JavaByteArrayToId<ray::NodeID>(env, node_id_bytes);
  auto node_resource_info = gcs_accessor->GetNodeResourceInfo(node_id);
  return static_cast<jbyteArray>(NativeStringToJavaByteArray(env, node_resource_info));
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetAllActorInfo(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto actor_info_list = gcs_accessor->GetAllActorInfo();
  return NativeVectorToJavaList<std::string>(
      env, actor_info_list, [](JNIEnv *env, const std::string &str) {
        return NativeStringToJavaByteArray(env, str);
      });
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetActorInfo(JNIEnv *env, jobject o,
                                                               jlong gcs_accessor_ptr,
                                                               jbyteArray actorId) {
  const auto actor_id = JavaByteArrayToId<ActorID>(env, actorId);
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto actor_info = gcs_accessor->GetActorInfo(actor_id);
  if (actor_info) {
    return NativeStringToJavaByteArray(env, *actor_info);
  }
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetPlacementGroupInfo(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr, jbyteArray placement_group_id_bytes) {
  const auto placement_group_id =
      JavaByteArrayToId<ray::PlacementGroupID>(env, placement_group_id_bytes);
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto placement_group = gcs_accessor->GetPlacementGroupInfo(placement_group_id);
  if (placement_group) {
    return NativeStringToJavaByteArray(env, *placement_group);
  }
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetPlacementGroupInfoByName(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr, jstring name, jboolean global) {
  std::string placement_group_name = JavaStringToNativeString(env, name);
  auto full_name = GetFullName(global, placement_group_name);
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto placement_group = gcs_accessor->GetPlacementGroupByName(full_name);
  if (placement_group) {
    return NativeStringToJavaByteArray(env, *placement_group);
  }
  return nullptr;
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_gcs_GlobalStateAccessor_nativeGetAllPlacementGroupInfo(
    JNIEnv *env, jobject o, jlong gcs_accessor_ptr) {
  auto *gcs_accessor =
      reinterpret_cast<ray::gcs::GlobalStateAccessor *>(gcs_accessor_ptr);
  auto placement_group_info_list = gcs_accessor->GetAllPlacementGroupInfo();
  return NativeVectorToJavaList<std::string>(
      env, placement_group_info_list, [](JNIEnv *env, const std::string &str) {
        return NativeStringToJavaByteArray(env, str);
      });
}

#ifdef __cplusplus
}
#endif
