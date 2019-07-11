#include "ray/raylet/lib/java/org_ray_runtime_raylet_RayletClientImpl.h"

#include <jni.h>

#include "ray/common/id.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/logging.h"

template <typename ID>
class UniqueIdFromJByteArray {
 public:
  const ID &GetId() const { return id; }

  UniqueIdFromJByteArray(JNIEnv *env, const jbyteArray &bytes) {
    std::string id_str(ID::Size(), 0);
    env->GetByteArrayRegion(bytes, 0, ID::Size(),
                            reinterpret_cast<jbyte *>(&id_str.front()));
    id = ID::FromBinary(id_str);
  }

 private:
  ID id;
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
    jbyteArray jobId) {
  UniqueIdFromJByteArray<ClientID> worker_id(env, workerId);
  UniqueIdFromJByteArray<JobID> job_id(env, jobId);
  const char *nativeString = env->GetStringUTFChars(sockName, JNI_FALSE);
  auto raylet_client = new RayletClient(nativeString, worker_id.GetId(), isWorker,
                                        job_id.GetId(), Language::JAVA);
  env->ReleaseStringUTFChars(sockName, nativeString);
  return reinterpret_cast<jlong>(raylet_client);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeSubmitTask
 * Signature: (J[BLjava/nio/ByteBuffer;II)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeSubmitTask(
    JNIEnv *env, jclass, jlong client, jbyteArray cursorId, jbyteArray taskSpec) {
  auto raylet_client = reinterpret_cast<RayletClient *>(client);

  std::vector<ObjectID> execution_dependencies;
  if (cursorId != nullptr) {
    UniqueIdFromJByteArray<ObjectID> cursor_id(env, cursorId);
    execution_dependencies.push_back(cursor_id.GetId());
  }

  jbyte *data = env->GetByteArrayElements(taskSpec, NULL);
  jsize size = env->GetArrayLength(taskSpec);
  ray::rpc::TaskSpec task_spec_message;
  task_spec_message.ParseFromArray(data, size);
  env->ReleaseByteArrayElements(taskSpec, data, JNI_ABORT);

  ray::TaskSpecification task_spec(task_spec_message);
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

  std::unique_ptr<ray::TaskSpecification> spec;
  auto status = raylet_client->GetTask(&spec);
  if (ThrowRayExceptionIfNotOK(env, status)) {
    return nullptr;
  }

  // Serialize the task spec and copy to Java byte array.
  auto task_data = spec->Serialize();

  jbyteArray result = env->NewByteArray(task_data.size());
  if (result == nullptr) {
    return nullptr; /* out of memory error thrown */
  }

  env->SetByteArrayRegion(result, 0, task_data.size(),
                          reinterpret_cast<const jbyte *>(task_data.data()));

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
    JNIEnv *env, jclass, jbyteArray jobId, jbyteArray parentTaskId,
    jint parent_task_counter) {
  UniqueIdFromJByteArray<JobID> job_id(env, jobId);
  UniqueIdFromJByteArray<TaskID> parent_task_id(env, parentTaskId);

  TaskID task_id =
      ray::GenerateTaskId(job_id.GetId(), parent_task_id.GetId(), parent_task_counter);
  jbyteArray result = env->NewByteArray(task_id.Size());
  if (nullptr == result) {
    return nullptr;
  }
  env->SetByteArrayRegion(result, 0, task_id.Size(),
                          reinterpret_cast<const jbyte *>(task_id.Data()));

  return result;
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeFreePlasmaObjects
 * Signature: (J[[BZZ)V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeFreePlasmaObjects(
    JNIEnv *env, jclass, jlong client, jobjectArray objectIds, jboolean localOnly,
    jboolean deleteCreatingTasks) {
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
  auto status = raylet_client->FreeObjects(object_ids, localOnly, deleteCreatingTasks);
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
  jbyteArray result = env->NewByteArray(checkpoint_id.Size());
  env->SetByteArrayRegion(result, 0, checkpoint_id.Size(),
                          reinterpret_cast<const jbyte *>(checkpoint_id.Data()));
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

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeSetResource
 * Signature: (JLjava/lang/String;D[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeSetResource(
    JNIEnv *env, jclass, jlong client, jstring resourceName, jdouble capacity,
    jbyteArray nodeId) {
  auto raylet_client = reinterpret_cast<RayletClient *>(client);
  UniqueIdFromJByteArray<ClientID> node_id(env, nodeId);
  const char *native_resource_name = env->GetStringUTFChars(resourceName, JNI_FALSE);

  auto status = raylet_client->SetResource(
      native_resource_name, static_cast<double>(capacity), node_id.GetId());
  env->ReleaseStringUTFChars(resourceName, native_resource_name);
  ThrowRayExceptionIfNotOK(env, status);
}

#ifdef __cplusplus
}
#endif
