#include "ray/raylet/lib/java/org_ray_runtime_raylet_RayletClientImpl.h"

#include <jni.h>

#include "ray/id.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/logging.h"

template <typename ID>
class UniqueIdFromJByteArray {
 public:
  const ID &GetId() const { return *id_pointer_; }

  UniqueIdFromJByteArray(JNIEnv *env, jbyteArray bytes) : env_(env), bytes_(bytes) {
    jbyte *b = reinterpret_cast<jbyte *>(env_->GetByteArrayElements(bytes_, nullptr));
    id_pointer_ = reinterpret_cast<ID *>(b);
  }

  ~UniqueIdFromJByteArray() {
    env_->ReleaseByteArrayElements(bytes_, reinterpret_cast<jbyte *>(id_pointer_), 0);
  }

 private:
  JNIEnv *env_;
  jbyteArray bytes_;
  ID *id_pointer_;
};

#ifdef __cplusplus
extern "C" {
#endif

inline bool ThrowRayExceptionIfNotOK(JNIEnv *env, const ray::Status &status) {
  if (!status.ok()) {
    jclass exception_class = env->FindClass("org/ray/api/exception/RayException");
    env->ThrowNew(exception_class, status.message().c_str());
    return true;
  } else {
    return false;
  }
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeInit
 * Signature: (Ljava/lang/String;[BZ[B)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeInit(
    JNIEnv *env, jclass, jstring sockName, jbyteArray workerId, jboolean isWorker,
    jbyteArray driverId) {
  UniqueIdFromJByteArray<ClientID> worker_id(env, workerId);
  UniqueIdFromJByteArray<DriverID> driver_id(env, driverId);
  const char *nativeString = env->GetStringUTFChars(sockName, JNI_FALSE);
  auto raylet_client = new RayletClient(nativeString, worker_id.GetId(), isWorker,
                                        driver_id.GetId(), Language::JAVA);
  env->ReleaseStringUTFChars(sockName, nativeString);
  return reinterpret_cast<jlong>(raylet_client);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeSubmitTask
 * Signature: (J[BLjava/nio/ByteBuffer;II)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeSubmitTask(
    JNIEnv *env, jclass, jlong client, jbyteArray cursorId, jobject taskBuff, jint pos,
    jint taskSize) {
  auto raylet_client = reinterpret_cast<RayletClient *>(client);

  std::vector<ObjectID> execution_dependencies;
  if (cursorId != nullptr) {
    UniqueIdFromJByteArray<ObjectID> cursor_id(env, cursorId);
    execution_dependencies.push_back(cursor_id.GetId());
  }

  auto data = reinterpret_cast<char *>(env->GetDirectBufferAddress(taskBuff)) + pos;
  ray::raylet::TaskSpecification task_spec(std::string(data, taskSize));
  auto status = raylet_client->SubmitTask(execution_dependencies, task_spec);
  ThrowRayExceptionIfNotOK(env, status);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeGetTask
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeGetTask(
    JNIEnv *env, jclass, jlong client) {
  auto raylet_client = reinterpret_cast<RayletClient *>(client);

  std::unique_ptr<ray::raylet::TaskSpecification> spec;
  auto status = raylet_client->GetTask(&spec);
  if (ThrowRayExceptionIfNotOK(env, status)) {
    return nullptr;
  }

  // We serialize the task specification using flatbuffers and then parse the
  // resulting string. This awkwardness is due to the fact that the Java
  // implementation does not use the underlying C++ TaskSpecification class.
  flatbuffers::FlatBufferBuilder fbb;
  auto message = spec->ToFlatbuffer(fbb);
  fbb.Finish(message);
  auto task_message = flatbuffers::GetRoot<flatbuffers::String>(fbb.GetBufferPointer());

  jbyteArray result;
  result = env->NewByteArray(task_message->size());
  if (result == nullptr) {
    return nullptr; /* out of memory error thrown */
  }

  // move from task spec structure to the java structure
  env->SetByteArrayRegion(
      result, 0, task_message->size(),
      reinterpret_cast<jbyte *>(const_cast<char *>(task_message->data())));

  return result;
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeDestroy(
    JNIEnv *env, jclass, jlong client) {
  auto raylet_client = reinterpret_cast<RayletClient *>(client);
  ThrowRayExceptionIfNotOK(env, raylet_client->Disconnect());
  delete raylet_client;
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeFetchOrReconstruct
 * Signature: (J[[BZ[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeFetchOrReconstruct(
    JNIEnv *env, jclass, jlong client, jobjectArray objectIds, jboolean fetchOnly,
    jbyteArray currentTaskId) {
  std::vector<ObjectID> object_ids;
  auto len = env->GetArrayLength(objectIds);
  for (int i = 0; i < len; i++) {
    jbyteArray object_id_bytes =
        static_cast<jbyteArray>(env->GetObjectArrayElement(objectIds, i));
    UniqueIdFromJByteArray<ObjectID> object_id(env, object_id_bytes);
    object_ids.push_back(object_id.GetId());
    env->DeleteLocalRef(object_id_bytes);
  }
  UniqueIdFromJByteArray<TaskID> current_task_id(env, currentTaskId);
  auto raylet_client = reinterpret_cast<RayletClient *>(client);
  auto status =
      raylet_client->FetchOrReconstruct(object_ids, fetchOnly, current_task_id.GetId());
  ThrowRayExceptionIfNotOK(env, status);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeNotifyUnblocked
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeNotifyUnblocked(
    JNIEnv *env, jclass, jlong client, jbyteArray currentTaskId) {
  UniqueIdFromJByteArray<TaskID> current_task_id(env, currentTaskId);
  auto raylet_client = reinterpret_cast<RayletClient *>(client);
  auto status = raylet_client->NotifyUnblocked(current_task_id.GetId());
  ThrowRayExceptionIfNotOK(env, status);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeWaitObject
 * Signature: (J[[BIIZ[B)[Z
 */
JNIEXPORT jbooleanArray JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeWaitObject(
    JNIEnv *env, jclass, jlong client, jobjectArray objectIds, jint numReturns,
    jint timeoutMillis, jboolean isWaitLocal, jbyteArray currentTaskId) {
  std::vector<ObjectID> object_ids;
  auto len = env->GetArrayLength(objectIds);
  for (int i = 0; i < len; i++) {
    jbyteArray object_id_bytes =
        static_cast<jbyteArray>(env->GetObjectArrayElement(objectIds, i));
    UniqueIdFromJByteArray<ObjectID> object_id(env, object_id_bytes);
    object_ids.push_back(object_id.GetId());
    env->DeleteLocalRef(object_id_bytes);
  }
  UniqueIdFromJByteArray<TaskID> current_task_id(env, currentTaskId);

  auto raylet_client = reinterpret_cast<RayletClient *>(client);

  // Invoke wait.
  WaitResultPair result;
  auto status = raylet_client->Wait(object_ids, numReturns, timeoutMillis,
                                    static_cast<bool>(isWaitLocal),
                                    current_task_id.GetId(), &result);
  if (ThrowRayExceptionIfNotOK(env, status)) {
    return nullptr;
  }

  // Convert result to java object.
  jboolean put_value = true;
  jbooleanArray resultArray = env->NewBooleanArray(object_ids.size());
  for (uint i = 0; i < result.first.size(); ++i) {
    for (uint j = 0; j < object_ids.size(); ++j) {
      if (result.first[i] == object_ids[j]) {
        env->SetBooleanArrayRegion(resultArray, j, 1, &put_value);
        break;
      }
    }
  }

  put_value = false;
  for (uint i = 0; i < result.second.size(); ++i) {
    for (uint j = 0; j < object_ids.size(); ++j) {
      if (result.second[i] == object_ids[j]) {
        env->SetBooleanArrayRegion(resultArray, j, 1, &put_value);
        break;
      }
    }
  }
  return resultArray;
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeGenerateTaskId
 * Signature: ([B[BI)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeGenerateTaskId(
    JNIEnv *env, jclass, jbyteArray driverId, jbyteArray parentTaskId,
    jint parent_task_counter) {
  UniqueIdFromJByteArray<DriverID> driver_id(env, driverId);
  UniqueIdFromJByteArray<TaskID> parent_task_id(env, parentTaskId);

  TaskID task_id =
      ray::GenerateTaskId(driver_id.GetId(), parent_task_id.GetId(), parent_task_counter);
  jbyteArray result = env->NewByteArray(sizeof(TaskID));
  if (nullptr == result) {
    return nullptr;
  }
  env->SetByteArrayRegion(result, 0, sizeof(TaskID), reinterpret_cast<jbyte *>(&task_id));

  return result;
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeFreePlasmaObjects
 * Signature: ([[BZ)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeFreePlasmaObjects(
    JNIEnv *env, jclass, jlong client, jobjectArray objectIds, jboolean localOnly) {
  std::vector<ObjectID> object_ids;
  auto len = env->GetArrayLength(objectIds);
  for (int i = 0; i < len; i++) {
    jbyteArray object_id_bytes =
        static_cast<jbyteArray>(env->GetObjectArrayElement(objectIds, i));
    UniqueIdFromJByteArray<ObjectID> object_id(env, object_id_bytes);
    object_ids.push_back(object_id.GetId());
    env->DeleteLocalRef(object_id_bytes);
  }
  auto raylet_client = reinterpret_cast<RayletClient *>(client);
  auto status = raylet_client->FreeObjects(object_ids, localOnly);
  ThrowRayExceptionIfNotOK(env, status);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativePrepareCheckpoint
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativePrepareCheckpoint(JNIEnv *env, jclass,
                                                                     jlong client,
                                                                     jbyteArray actorId) {
  auto raylet_client = reinterpret_cast<RayletClient *>(client);
  UniqueIdFromJByteArray<ActorID> actor_id(env, actorId);
  ActorCheckpointID checkpoint_id;
  auto status = raylet_client->PrepareActorCheckpoint(actor_id.GetId(), checkpoint_id);
  if (ThrowRayExceptionIfNotOK(env, status)) {
    return nullptr;
  }
  jbyteArray result = env->NewByteArray(sizeof(ActorCheckpointID));
  env->SetByteArrayRegion(result, 0, sizeof(ActorCheckpointID),
                          reinterpret_cast<jbyte *>(&checkpoint_id));
  return result;
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeNotifyActorResumedFromCheckpoint
 * Signature: (J[B[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeNotifyActorResumedFromCheckpoint(
    JNIEnv *env, jclass, jlong client, jbyteArray actorId, jbyteArray checkpointId) {
  auto raylet_client = reinterpret_cast<RayletClient *>(client);
  UniqueIdFromJByteArray<ActorID> actor_id(env, actorId);
  UniqueIdFromJByteArray<ActorCheckpointID> checkpoint_id(env, checkpointId);
  auto status = raylet_client->NotifyActorResumedFromCheckpoint(actor_id.GetId(),
                                                                checkpoint_id.GetId());
  ThrowRayExceptionIfNotOK(env, status);
}

#ifdef __cplusplus
}
#endif
