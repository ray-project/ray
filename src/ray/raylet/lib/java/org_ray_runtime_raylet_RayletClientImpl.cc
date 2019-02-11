#include "ray/raylet/lib/java/org_ray_runtime_raylet_RayletClientImpl.h"

#include <jni.h>

#include "ray/id.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/logging.h"

#ifdef __cplusplus
extern "C" {
#endif

class UniqueIdFromJByteArray {
 private:
  JNIEnv *_env;
  jbyteArray _bytes;

 public:
  UniqueID *PID;

  UniqueIdFromJByteArray(JNIEnv *env, jbyteArray wid) {
    _env = env;
    _bytes = wid;

    jbyte *b = reinterpret_cast<jbyte *>(_env->GetByteArrayElements(_bytes, nullptr));
    PID = reinterpret_cast<UniqueID *>(b);
  }

  ~UniqueIdFromJByteArray() {
    _env->ReleaseByteArrayElements(_bytes, reinterpret_cast<jbyte *>(PID), 0);
  }
};

inline void ThrowRayExceptionIfNotOK(JNIEnv *env, const ray::Status &status,
                                     const std::string &message) {
  if (!status.ok()) {
    jclass exception_class = env->FindClass("org/ray/api/exception/RayException");
    env->ThrowNew(exception_class, message.c_str());
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
  UniqueIdFromJByteArray worker_id(env, workerId);
  UniqueIdFromJByteArray driver_id(env, driverId);
  const char *nativeString = env->GetStringUTFChars(sockName, JNI_FALSE);
  auto raylet_client = new RayletClient(nativeString, *worker_id.PID, isWorker,
                                        *driver_id.PID, Language::JAVA);
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
    UniqueIdFromJByteArray cursor_id(env, cursorId);
    execution_dependencies.push_back(*cursor_id.PID);
  }

  auto data = reinterpret_cast<char *>(env->GetDirectBufferAddress(taskBuff)) + pos;
  ray::raylet::TaskSpecification task_spec(std::string(data, taskSize));
  auto status = raylet_client->SubmitTask(execution_dependencies, task_spec);
  ThrowRayExceptionIfNotOK(env, status,
                           "[RayletClient] Failed to submit a task to raylet.");
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeGetTask
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeGetTask(
    JNIEnv *env, jclass, jlong client) {
  auto raylet_client = reinterpret_cast<RayletClient *>(client);

  // TODO: handle actor failure later
  std::unique_ptr<ray::raylet::TaskSpecification> spec;
  auto status = raylet_client->GetTask(&spec);
  ThrowRayExceptionIfNotOK(env, status,
                           "[RayletClient] Failed to get a task from raylet.");

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
  ThrowRayExceptionIfNotOK(env, raylet_client->Disconnect(),
                           "[RayletClient] Failed to disconnect.");
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
    UniqueIdFromJByteArray object_id(env, object_id_bytes);
    object_ids.push_back(*object_id.PID);
    env->DeleteLocalRef(object_id_bytes);
  }
  UniqueIdFromJByteArray current_task_id(env, currentTaskId);
  auto raylet_client = reinterpret_cast<RayletClient *>(client);
  auto status =
      raylet_client->FetchOrReconstruct(object_ids, fetchOnly, *current_task_id.PID);
  ThrowRayExceptionIfNotOK(env, status, "[RayletClient] Failed to fetch or reconstruct.");
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeNotifyUnblocked
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeNotifyUnblocked(
    JNIEnv *env, jclass, jlong client, jbyteArray currentTaskId) {
  UniqueIdFromJByteArray current_task_id(env, currentTaskId);
  auto raylet_client = reinterpret_cast<RayletClient *>(client);
  auto status = raylet_client->NotifyUnblocked(*current_task_id.PID);
  ThrowRayExceptionIfNotOK(env, status, "[RayletClient] Failed to notify unblocked.");
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
    UniqueIdFromJByteArray object_id(env, object_id_bytes);
    object_ids.push_back(*object_id.PID);
    env->DeleteLocalRef(object_id_bytes);
  }
  UniqueIdFromJByteArray current_task_id(env, currentTaskId);

  auto raylet_client = reinterpret_cast<RayletClient *>(client);

  // Invoke wait.
  WaitResultPair result;
  auto status =
      raylet_client->Wait(object_ids, numReturns, timeoutMillis,
                          static_cast<bool>(isWaitLocal), *current_task_id.PID, &result);
  ThrowRayExceptionIfNotOK(env, status, "[RayletClient] Failed to wait for objects.");

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
  UniqueIdFromJByteArray object_id1(env, driverId);
  ray::DriverID driver_id = *object_id1.PID;

  UniqueIdFromJByteArray object_id2(env, parentTaskId);
  ray::TaskID parent_task_id = *object_id2.PID;

  ray::TaskID task_id =
      ray::GenerateTaskId(driver_id, parent_task_id, parent_task_counter);
  jbyteArray result = env->NewByteArray(sizeof(ray::TaskID));
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
    UniqueIdFromJByteArray object_id(env, object_id_bytes);
    object_ids.push_back(*object_id.PID);
    env->DeleteLocalRef(object_id_bytes);
  }
  auto raylet_client = reinterpret_cast<RayletClient *>(client);
  auto status = raylet_client->FreeObjects(object_ids, localOnly);
  ThrowRayExceptionIfNotOK(env, status, "[RayletClient] Failed to free objects.");
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
  UniqueIdFromJByteArray actor_id(env, actorId);
  ActorCheckpointID checkpoint_id;
  RAY_CHECK_OK(raylet_client->PrepareActorCheckpoint(*actor_id.PID, checkpoint_id));
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
  UniqueIdFromJByteArray actor_id(env, actorId);
  UniqueIdFromJByteArray checkpoint_id(env, checkpointId);
  RAY_CHECK_OK(
      raylet_client->NotifyActorResumedFromCheckpoint(*actor_id.PID, *checkpoint_id.PID));
}

#ifdef __cplusplus
}
#endif
