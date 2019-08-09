#include "ray/raylet/lib/java/org_ray_runtime_raylet_RayletClientImpl.h"

#include <jni.h>

#include "ray/common/id.h"
#include "ray/core_worker/lib/java/jni_utils.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/logging.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeInit
 * Signature: (Ljava/lang/String;[BZ[B)J
 */
JNIEXPORT jlong JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeInit(
    JNIEnv *env, jclass, jstring sockName, jbyteArray workerId, jboolean isWorker,
    jbyteArray jobId) {
  const auto worker_id = JavaByteArrayToId<ClientID>(env, workerId);
  const auto job_id = JavaByteArrayToId<JobID>(env, jobId);
  const char *nativeString = env->GetStringUTFChars(sockName, JNI_FALSE);
  auto raylet_client = new std::unique_ptr<RayletClient>(
      new RayletClient(nativeString, worker_id, isWorker, job_id, Language::JAVA));
  env->ReleaseStringUTFChars(sockName, nativeString);
  return reinterpret_cast<jlong>(raylet_client);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeSubmitTask
 * Signature: (J[BLjava/nio/ByteBuffer;II)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeSubmitTask(
    JNIEnv *env, jclass, jlong client, jbyteArray taskSpec) {
  auto &raylet_client = *reinterpret_cast<std::unique_ptr<RayletClient> *>(client);

  jbyte *data = env->GetByteArrayElements(taskSpec, NULL);
  jsize size = env->GetArrayLength(taskSpec);
  ray::rpc::TaskSpec task_spec_message;
  task_spec_message.ParseFromArray(data, size);
  env->ReleaseByteArrayElements(taskSpec, data, JNI_ABORT);

  ray::TaskSpecification task_spec(task_spec_message);
  auto status = raylet_client->SubmitTask(task_spec);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeGetTask
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeGetTask(
    JNIEnv *env, jclass, jlong client) {
  auto &raylet_client = *reinterpret_cast<std::unique_ptr<RayletClient> *>(client);

  std::unique_ptr<ray::TaskSpecification> spec;
  auto status = raylet_client->GetTask(&spec);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);

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
  auto raylet_client = reinterpret_cast<std::unique_ptr<RayletClient> *>(client);
  auto status = (*raylet_client)->Disconnect();
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
  delete raylet_client;
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
    const auto object_id = JavaByteArrayToId<ObjectID>(env, object_id_bytes);
    object_ids.push_back(object_id);
    env->DeleteLocalRef(object_id_bytes);
  }
  const auto current_task_id = JavaByteArrayToId<TaskID>(env, currentTaskId);

  auto &raylet_client = *reinterpret_cast<std::unique_ptr<RayletClient> *>(client);

  // Invoke wait.
  WaitResultPair result;
  auto status =
      raylet_client->Wait(object_ids, numReturns, timeoutMillis,
                          static_cast<bool>(isWaitLocal), current_task_id, &result);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);

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
 * Method:    nativeGenerateActorCreationTaskId
 * Signature: ([B[BI)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeGenerateActorCreationTaskId(
    JNIEnv *env, jclass, jbyteArray jobId, jbyteArray parentTaskId,
    jint parent_task_counter) {
  const auto job_id = JavaByteArrayToId<JobID>(env, jobId);
  const auto parent_task_id = JavaByteArrayToId<TaskID>(env, parentTaskId);

  const ActorID actor_id = ray::ActorID::Of(job_id, parent_task_id, parent_task_counter);
  const TaskID actor_creation_task_id = ray::TaskID::ForActorCreationTask(actor_id);
  jbyteArray result = env->NewByteArray(actor_creation_task_id.Size());
  if (nullptr == result) {
    return nullptr;
  }
  env->SetByteArrayRegion(result, 0, actor_creation_task_id.Size(),
                          reinterpret_cast<const jbyte *>(actor_creation_task_id.Data()));
  return result;
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeGenerateActorTaskId
 * Signature: ([B[BI[B)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeGenerateActorTaskId(
    JNIEnv *env, jclass, jbyteArray jobId, jbyteArray parentTaskId,
    jint parent_task_counter, jbyteArray actorId) {
  const auto job_id = JavaByteArrayToId<JobID>(env, jobId);
  const auto parent_task_id = JavaByteArrayToId<TaskID>(env, parentTaskId);
  const auto actor_id = JavaByteArrayToId<ActorID>(env, actorId);
  const TaskID actor_task_id =
      ray::TaskID::ForActorTask(job_id, parent_task_id, parent_task_counter, actor_id);

  jbyteArray result = env->NewByteArray(actor_task_id.Size());
  if (nullptr == result) {
    return nullptr;
  }
  env->SetByteArrayRegion(result, 0, actor_task_id.Size(),
                          reinterpret_cast<const jbyte *>(actor_task_id.Data()));
  return result;
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeGenerateNormalTaskId
 * Signature: ([B[BI)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_runtime_raylet_RayletClientImpl_nativeGenerateNormalTaskId(
    JNIEnv *env, jclass, jbyteArray jobId, jbyteArray parentTaskId,
    jint parent_task_counter) {
  const auto job_id = JavaByteArrayToId<JobID>(env, jobId);
  const auto parent_task_id = JavaByteArrayToId<TaskID>(env, parentTaskId);
  const TaskID task_id =
      ray::TaskID::ForNormalTask(job_id, parent_task_id, parent_task_counter);

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
    const auto object_id = JavaByteArrayToId<ObjectID>(env, object_id_bytes);
    object_ids.push_back(object_id);
    env->DeleteLocalRef(object_id_bytes);
  }
  auto &raylet_client = *reinterpret_cast<std::unique_ptr<RayletClient> *>(client);
  auto status = raylet_client->FreeObjects(object_ids, localOnly, deleteCreatingTasks);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
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
  auto &raylet_client = *reinterpret_cast<std::unique_ptr<RayletClient> *>(client);
  const auto actor_id = JavaByteArrayToId<ActorID>(env, actorId);
  ActorCheckpointID checkpoint_id;
  auto status = raylet_client->PrepareActorCheckpoint(actor_id, checkpoint_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, nullptr);
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
  auto &raylet_client = *reinterpret_cast<std::unique_ptr<RayletClient> *>(client);
  const auto actor_id = JavaByteArrayToId<ActorID>(env, actorId);
  const auto checkpoint_id = JavaByteArrayToId<ActorCheckpointID>(env, checkpointId);
  auto status = raylet_client->NotifyActorResumedFromCheckpoint(actor_id, checkpoint_id);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeSetResource
 * Signature: (JLjava/lang/String;D[B)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeSetResource(
    JNIEnv *env, jclass, jlong client, jstring resourceName, jdouble capacity,
    jbyteArray nodeId) {
  auto &raylet_client = *reinterpret_cast<std::unique_ptr<RayletClient> *>(client);
  const auto node_id = JavaByteArrayToId<ClientID>(env, nodeId);
  const char *native_resource_name = env->GetStringUTFChars(resourceName, JNI_FALSE);

  auto status = raylet_client->SetResource(native_resource_name,
                                           static_cast<double>(capacity), node_id);
  env->ReleaseStringUTFChars(resourceName, native_resource_name);
  THROW_EXCEPTION_AND_RETURN_IF_NOT_OK(env, status, (void)0);
}

#ifdef __cplusplus
}
#endif
