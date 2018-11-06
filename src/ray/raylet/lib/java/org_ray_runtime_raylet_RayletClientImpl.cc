#include "ray/raylet/lib/java/org_ray_runtime_raylet_RayletClientImpl.h"

#include <jni.h>

#include "ray/id.h"
#include "ray/raylet/local_scheduler_client.h"
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
  auto client = LocalSchedulerConnection_init(nativeString, *worker_id.PID, isWorker,
                                              *driver_id.PID, Language::JAVA);
  env->ReleaseStringUTFChars(sockName, nativeString);
  return reinterpret_cast<jlong>(client);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeSubmitTask
 * Signature: (J[BLjava/nio/ByteBuffer;II)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeSubmitTask(
    JNIEnv *env, jclass, jlong client, jbyteArray cursorId, jobject taskBuff, jint pos,
    jint taskSize) {
  auto conn = reinterpret_cast<LocalSchedulerConnection *>(client);

  std::vector<ObjectID> execution_dependencies;
  if (cursorId != nullptr) {
    UniqueIdFromJByteArray cursor_id(env, cursorId);
    execution_dependencies.push_back(*cursor_id.PID);
  }

  auto data = reinterpret_cast<char *>(env->GetDirectBufferAddress(taskBuff)) + pos;
  ray::raylet::TaskSpecification task_spec(std::string(data, taskSize));
  local_scheduler_submit_raylet(conn, execution_dependencies, task_spec);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeGetTask
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeGetTask(
    JNIEnv *env, jclass, jlong client) {
  auto conn = reinterpret_cast<LocalSchedulerConnection *>(client);

  // TODO: handle actor failure later
  ray::raylet::TaskSpecification *spec = local_scheduler_get_task_raylet(conn);

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

  delete spec;
  return result;
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeDestroy(
    JNIEnv *, jclass, jlong client) {
  auto conn = reinterpret_cast<LocalSchedulerConnection *>(client);
  local_scheduler_disconnect_client(conn);
  LocalSchedulerConnection_free(conn);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeFetchOrReconstruct
 * Signature: (J[[BZ)V
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
  auto conn = reinterpret_cast<LocalSchedulerConnection *>(client);
  local_scheduler_fetch_or_reconstruct(conn, object_ids, fetchOnly, *current_task_id.PID);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeNotifyUnblocked
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_raylet_RayletClientImpl_nativeNotifyUnblocked(
    JNIEnv *env, jclass, jlong client, jbyteArray currentTaskId) {
  UniqueIdFromJByteArray current_task_id(env, currentTaskId);
  auto conn = reinterpret_cast<LocalSchedulerConnection *>(client);
  local_scheduler_notify_unblocked(conn, *current_task_id.PID);
}

/*
 * Class:     org_ray_runtime_raylet_RayletClientImpl
 * Method:    nativeWaitObject
 * Signature: (J[[BIIZ)[Z
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

  auto conn = reinterpret_cast<LocalSchedulerConnection *>(client);

  // Invoke wait.
  std::pair<std::vector<ObjectID>, std::vector<ObjectID>> result =
      local_scheduler_wait(conn, object_ids, numReturns, timeoutMillis,
                           static_cast<bool>(isWaitLocal), *current_task_id.PID);

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
  auto conn = reinterpret_cast<LocalSchedulerConnection *>(client);
  local_scheduler_free_objects_in_object_store(conn, object_ids, localOnly);
}

#ifdef __cplusplus
}
#endif
