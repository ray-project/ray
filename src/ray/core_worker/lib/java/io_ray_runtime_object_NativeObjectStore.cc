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

#include "jni_utils.h"
#include "ray/common/id.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/core_worker.h"
#include "ray/gcs/gcs_client/global_state_accessor.h"

Status PutSerializedObject(JNIEnv *env,
                           jobject obj,
                           ObjectID object_id,
                           ObjectID *out_object_id,
                           bool pin_object = true,
                           const std::unique_ptr<rpc::Address> &owner_address = nullptr) {
  auto native_ray_object = JavaNativeRayObjectToNativeRayObject(env, obj);
  RAY_CHECK(native_ray_object != nullptr);
  size_t data_size = 0;
  if (native_ray_object->HasData()) {
    data_size = native_ray_object->GetData()->Size();
  }
  std::shared_ptr<Buffer> data;
  Status status;
  if (object_id.IsNil()) {
    std::vector<ObjectID> nested_ids;
    for (const auto &ref : native_ray_object->GetNestedRefs()) {
      nested_ids.push_back(ObjectID::FromBinary(ref.object_id()));
    }
    status = CoreWorkerProcess::GetCoreWorker().CreateOwnedAndIncrementLocalRef(
        native_ray_object->GetMetadata(),
        data_size,
        nested_ids,
        out_object_id,
        &data,
        /*created_by_worker=*/true,
        /*owner_address=*/owner_address);
  } else {
    status = CoreWorkerProcess::GetCoreWorker().CreateExisting(
        native_ray_object->GetMetadata(),
        data_size,
        object_id,
        CoreWorkerProcess::GetCoreWorker().GetRpcAddress(),
        &data,
        /*created_by_worker=*/true);
    *out_object_id = object_id;
  }
  if (!status.ok()) {
    return status;
  }
  // If data is nullptr, that means the ObjectID already existed, which we ignore.
  // TODO(edoakes): this is hacky, we should return the error instead and deal with it
  // here.
  if (data != nullptr) {
    if (data->Size() > 0) {
      memcpy(data->Data(), native_ray_object->GetData()->Data(), data->Size());
    }
    if (object_id.IsNil()) {
      RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().SealOwned(
          *out_object_id, pin_object, owner_address));
    } else {
      RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().SealExisting(
          *out_object_id,
          /* pin_object = */ false,
          /* generator_id = */ ObjectID::Nil(),
          owner_address));
    }
  }
  return Status::OK();
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_object_NativeObjectStore_nativePut__Lio_ray_runtime_object_NativeRayObject_2_3B(
    JNIEnv *env, jclass, jobject obj, jbyteArray serialized_owner_actor_address_bytes) {
  ObjectID object_id;
  std::unique_ptr<rpc::Address> owner_address = nullptr;
  if (serialized_owner_actor_address_bytes != nullptr) {
    owner_address = std::make_unique<rpc::Address>();
    owner_address->ParseFromString(
        JavaByteArrayToNativeString(env, serialized_owner_actor_address_bytes));
  }
  auto status = PutSerializedObject(env,
                                    obj,
                                    /*object_id=*/ObjectID::Nil(),
                                    /*out_object_id=*/&object_id,
                                    /*pin_object=*/true,
                                    /*owner_address=*/owner_address);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return IdToJavaByteArray<ObjectID>(env, object_id);
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_object_NativeObjectStore_nativePut___3BLio_ray_runtime_object_NativeRayObject_2(
    JNIEnv *env, jclass, jbyteArray objectId, jobject obj) {
  auto object_id = JavaByteArrayToId<ObjectID>(env, objectId);
  ObjectID dummy_object_id;
  auto status = PutSerializedObject(env,
                                    obj,
                                    object_id,
                                    /*out_object_id=*/&dummy_object_id,
                                    /*pin_object=*/true);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT jobject JNICALL Java_io_ray_runtime_object_NativeObjectStore_nativeGet(
    JNIEnv *env, jclass, jobject ids, jlong timeoutMs) {
  std::vector<ObjectID> object_ids;
  JavaListToNativeVector<ObjectID>(env, ids, &object_ids, [](JNIEnv *env, jobject id) {
    return JavaByteArrayToId<ObjectID>(env, static_cast<jbyteArray>(id));
  });
  std::vector<std::shared_ptr<RayObject>> results;
  auto status =
      CoreWorkerProcess::GetCoreWorker().Get(object_ids, (int64_t)timeoutMs, &results);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeVectorToJavaList<std::shared_ptr<RayObject>>(
      env, results, NativeRayObjectToJavaNativeRayObject);
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_object_NativeObjectStore_nativeWait(JNIEnv *env,
                                                        jclass,
                                                        jobject objectIds,
                                                        jint numObjects,
                                                        jlong timeoutMs,
                                                        jboolean fetch_local) {
  std::vector<ObjectID> object_ids;
  JavaListToNativeVector<ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ObjectID>(env, static_cast<jbyteArray>(id));
      });
  std::vector<bool> results;
  auto status = CoreWorkerProcess::GetCoreWorker().Wait(
      object_ids, (int)numObjects, (int64_t)timeoutMs, &results, (bool)fetch_local);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
  return NativeVectorToJavaList<bool>(env, results, [](JNIEnv *env, const bool &item) {
    jobject java_item =
        env->NewObject(java_boolean_class, java_boolean_init, (jboolean)item);
    RAY_CHECK_JAVA_EXCEPTION(env);
    return java_item;
  });
}

JNIEXPORT void JNICALL Java_io_ray_runtime_object_NativeObjectStore_nativeDelete(
    JNIEnv *env, jclass, jobject objectIds, jboolean localOnly) {
  std::vector<ObjectID> object_ids;
  JavaListToNativeVector<ObjectID>(
      env, objectIds, &object_ids, [](JNIEnv *env, jobject id) {
        return JavaByteArrayToId<ObjectID>(env, static_cast<jbyteArray>(id));
      });
  auto status = CoreWorkerProcess::GetCoreWorker().Delete(object_ids, (bool)localOnly);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_object_NativeObjectStore_nativeAddLocalReference(
    JNIEnv *env, jclass, jbyteArray objectId) {
  auto object_id = JavaByteArrayToId<ObjectID>(env, objectId);
  auto core_worker = CoreWorkerProcess::TryGetWorker();
  RAY_CHECK(core_worker);
  core_worker->AddLocalReference(object_id);
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_object_NativeObjectStore_nativeRemoveLocalReference(
    JNIEnv *env, jclass, jbyteArray objectId) {
  auto object_id = JavaByteArrayToId<ObjectID>(env, objectId);
  // We can't control the timing of Java GC, so it's normal that this method is called but
  // core worker is shutting down (or already shut down). If we can't get a core worker
  // instance here, skip calling the `RemoveLocalReference` method.
  auto core_worker = CoreWorkerProcess::TryGetWorker();
  if (core_worker) {
    core_worker->RemoveLocalReference(object_id);
  }
}

JNIEXPORT jobject JNICALL
Java_io_ray_runtime_object_NativeObjectStore_nativeGetAllReferenceCounts(JNIEnv *env,
                                                                         jclass) {
  auto reference_counts = CoreWorkerProcess::GetCoreWorker().GetAllReferenceCounts();
  return NativeMapToJavaMap<ObjectID, std::pair<size_t, size_t>>(
      env,
      reference_counts,
      [](JNIEnv *env, const ObjectID &key) {
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

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_object_NativeObjectStore_nativeGetOwnerAddress(JNIEnv *env,
                                                                   jclass,
                                                                   jbyteArray objectId) {
  auto object_id = JavaByteArrayToId<ObjectID>(env, objectId);
  rpc::Address owner_address;
  // Ignore the outcome for now.
  const auto &rpc_address =
      CoreWorkerProcess::GetCoreWorker().GetOwnerAddressOrDie(object_id);
  return NativeStringToJavaByteArray(env, owner_address.SerializeAsString());
}

JNIEXPORT jbyteArray JNICALL
Java_io_ray_runtime_object_NativeObjectStore_nativeGetOwnershipInfo(JNIEnv *env,
                                                                    jclass,
                                                                    jbyteArray objectId) {
  auto object_id = JavaByteArrayToId<ObjectID>(env, objectId);
  rpc::Address address;
  // TODO(ekl) send serialized object status to Java land.
  std::string serialized_object_status;
  CoreWorkerProcess::GetCoreWorker().GetOwnershipInfo(
      object_id, &address, &serialized_object_status);
  auto address_str = address.SerializeAsString();
  auto arr = NativeStringToJavaByteArray(env, address_str);
  return arr;
}

JNIEXPORT void JNICALL
Java_io_ray_runtime_object_NativeObjectStore_nativeRegisterOwnershipInfoAndResolveFuture(
    JNIEnv *env,
    jclass,
    jbyteArray objectId,
    jbyteArray outerObjectId,
    jbyteArray ownerAddress) {
  auto object_id = JavaByteArrayToId<ObjectID>(env, objectId);
  auto outer_objectId = ObjectID::Nil();
  if (outerObjectId != NULL) {
    outer_objectId = JavaByteArrayToId<ObjectID>(env, outerObjectId);
  }
  auto ownerAddressStr = JavaByteArrayToNativeString(env, ownerAddress);
  rpc::Address address;
  address.ParseFromString(ownerAddressStr);
  // TODO(ekl) populate serialized object status from Java land.
  rpc::GetObjectStatusReply object_status;
  auto serialized_status = object_status.SerializeAsString();
  CoreWorkerProcess::GetCoreWorker().RegisterOwnershipInfoAndResolveFuture(
      object_id, outer_objectId, address, serialized_status);
}

#ifdef __cplusplus
}
#endif
