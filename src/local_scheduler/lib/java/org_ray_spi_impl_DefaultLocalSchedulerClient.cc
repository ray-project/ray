#include <jni.h>

#include "local_scheduler/lib/java/org_ray_spi_impl_DefaultLocalSchedulerClient.h"
#include "local_scheduler_client.h"
#include "logging.h"
#include "ray/id.h"

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

    jbyte *b =
        reinterpret_cast<jbyte *>(_env->GetByteArrayElements(_bytes, nullptr));
    PID = reinterpret_cast<UniqueID *>(b);
  }

  ~UniqueIdFromJByteArray() {
    _env->ReleaseByteArrayElements(_bytes, reinterpret_cast<jbyte *>(PID), 0);
  }
};

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _init
 * Signature: (Ljava/lang/String;[B[BZJ)J
 */
JNIEXPORT jlong JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1init(JNIEnv *env,
                                                         jclass,
                                                         jstring sockName,
                                                         jbyteArray wid,
                                                         jbyteArray actorId,
                                                         jboolean isWorker,
                                                         jbyteArray driverId,
                                                         jlong numGpus,
                                                         jboolean useRaylet) {
  // 	native private static long _init(String localSchedulerSocket,
  //     byte[] workerId, byte[] actorId, boolean isWorker, long numGpus);
  UniqueIdFromJByteArray worker_id(env, wid);
  UniqueIdFromJByteArray driver_id(env, driverId);
  const char *nativeString = env->GetStringUTFChars(sockName, JNI_FALSE);
  auto client =
      LocalSchedulerConnection_init(nativeString, *worker_id.PID, isWorker,
                                    *driver_id.PID, useRaylet, Language::JAVA);
  env->ReleaseStringUTFChars(sockName, nativeString);
  return reinterpret_cast<jlong>(client);
}

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _submitTask
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1submitTask(
    JNIEnv *env,
    jclass,
    jlong c,
    jbyteArray cursorId,
    jobject buff,
    jint pos,
    jint sz,
    jboolean useRaylet) {
  // task -> TaskInfo (with FlatBuffer)
  // native private static void _submitTask(long client, /*Direct*/ByteBuffer
  // task);
  auto client = reinterpret_cast<LocalSchedulerConnection *>(c);

  std::vector<ObjectID> execution_dependencies;
  if (cursorId != nullptr) {
    UniqueIdFromJByteArray cursor_id(env, cursorId);
    execution_dependencies.push_back(*cursor_id.PID);
  }
  if (!useRaylet) {
    TaskSpec *task =
        reinterpret_cast<char *>(env->GetDirectBufferAddress(buff)) + pos;
    TaskExecutionSpec taskExecutionSpec =
        TaskExecutionSpec(execution_dependencies, task, sz);
    local_scheduler_submit(client, taskExecutionSpec);
  } else {
    auto data =
        reinterpret_cast<char *>(env->GetDirectBufferAddress(buff)) + pos;
    ray::raylet::TaskSpecification task_spec(std::string(data, sz));
    local_scheduler_submit_raylet(client, execution_dependencies, task_spec);
  }
}

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _getTaskTodo
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1getTaskTodo(
    JNIEnv *env,
    jclass,
    jlong c,
    jboolean useRaylet) {
  // native private static ByteBuffer _getTaskTodo(long client);
  auto client = reinterpret_cast<LocalSchedulerConnection *>(c);
  int64_t task_size = 0;

  // TODO: handle actor failure later
  TaskSpec *spec = !useRaylet
                       ? local_scheduler_get_task(client, &task_size)
                       : local_scheduler_get_task_raylet(client, &task_size);

  jbyteArray result;
  result = env->NewByteArray(task_size);
  if (result == nullptr) {
    return nullptr; /* out of memory error thrown */
  }

  // move from task spec structure to the java structure
  env->SetByteArrayRegion(result, 0, task_size,
                          reinterpret_cast<jbyte *>(spec));

  TaskSpec_free(spec);
  return result;
}

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _computePutId
 * Signature: (J[BI)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1computePutId(JNIEnv *env,
                                                                 jclass,
                                                                 jlong c,
                                                                 jbyteArray tid,
                                                                 jint index) {
  // native private static byte[] _computePutId(long client, byte[] taskId, int
  // putIndex);
  UniqueIdFromJByteArray task(env, tid);

  auto client = reinterpret_cast<LocalSchedulerConnection *>(c);
  ObjectID putId = task_compute_put_id(*task.PID, index);
  local_scheduler_put_object(client, *task.PID, putId);

  jbyteArray result;
  result = env->NewByteArray(sizeof(ObjectID));
  if (result == nullptr) {
    return nullptr; /* out of memory error thrown */
  }

  // move from task spec structure to the java structure
  env->SetByteArrayRegion(result, 0, sizeof(ObjectID),
                          reinterpret_cast<jbyte *>(&putId));
  return result;
}

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _destroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1destroy(JNIEnv *,
                                                            jclass,
                                                            jlong c) {
  // native private static void _destroy(long client);
  auto client = reinterpret_cast<LocalSchedulerConnection *>(c);
  local_scheduler_disconnect_client(client);
  LocalSchedulerConnection_free(client);
}

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _task_done
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1task_1done(JNIEnv *,
                                                               jclass,
                                                               jlong c) {
  // native private static void _task_done(long client);
  auto client = reinterpret_cast<LocalSchedulerConnection *>(c);
  local_scheduler_task_done(client);
}

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _reconstruct_objects
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1reconstruct_1objects(
    JNIEnv *env,
    jclass,
    jlong c,
    jobjectArray oids,
    jboolean fetch_only) {
  // native private static void _reconstruct_objects(long client, byte[][]
  // objectIds, boolean fetchOnly);

  std::vector<ObjectID> object_ids;
  auto len = env->GetArrayLength(oids);
  for (int i = 0; i < len; i++) {
    jbyteArray oid = (jbyteArray) env->GetObjectArrayElement(oids, i);
    UniqueIdFromJByteArray o(env, oid);
    object_ids.push_back(*o.PID);
    env->DeleteLocalRef(oid);
  }
  auto client = reinterpret_cast<LocalSchedulerConnection *>(c);
  local_scheduler_reconstruct_objects(client, object_ids, fetch_only);
}

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _notify_unblocked
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1notify_1unblocked(JNIEnv *,
                                                                      jclass,
                                                                      jlong c) {
  // native private static void _notify_unblocked(long client);
  auto client = reinterpret_cast<LocalSchedulerConnection *>(c);
  local_scheduler_notify_unblocked(client);
}

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _put_object
 * Signature: (J[B[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1put_1object(
    JNIEnv *env,
    jclass,
    jlong c,
    jbyteArray tid,
    jbyteArray oid) {
  // native private static void _put_object(long client, byte[] taskId, byte[]
  // objectId);
  UniqueIdFromJByteArray o(env, oid), t(env, tid);
  auto client = reinterpret_cast<LocalSchedulerConnection *>(c);
  local_scheduler_put_object(client, *t.PID, *o.PID);
}

JNIEXPORT jbooleanArray JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1waitObject(
    JNIEnv *env,
    jclass,
    jlong c,
    jobjectArray oids,
    jint num_returns,
    jint timeout_ms,
    jboolean wait_local) {
  std::vector<ObjectID> object_ids;
  auto len = env->GetArrayLength(oids);
  for (int i = 0; i < len; i++) {
    jbyteArray oid = (jbyteArray) env->GetObjectArrayElement(oids, i);
    UniqueIdFromJByteArray o(env, oid);
    object_ids.push_back(*o.PID);
    env->DeleteLocalRef(oid);
  }

  auto client = reinterpret_cast<LocalSchedulerConnection *>(c);

  // Invoke wait.
  std::pair<std::vector<ObjectID>, std::vector<ObjectID>> result =
      local_scheduler_wait(client, object_ids, num_returns, timeout_ms,
                           static_cast<bool>(wait_local));

  // Convert result to java object.
  jboolean putValue = true;
  jbooleanArray resultArray = env->NewBooleanArray(object_ids.size());
  for (uint i = 0; i < result.first.size(); ++i) {
    for (uint j = 0; j < object_ids.size(); ++j) {
      if (result.first[i] == object_ids[j]) {
        env->SetBooleanArrayRegion(resultArray, j, 1, &putValue);
        break;
      }
    }
  }

  putValue = false;
  for (uint i = 0; i < result.second.size(); ++i) {
    for (uint j = 0; j < object_ids.size(); ++j) {
      if (result.second[i] == object_ids[j]) {
        env->SetBooleanArrayRegion(resultArray, j, 1, &putValue);
        break;
      }
    }
  }
  return resultArray;
}

JNIEXPORT jbyteArray JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1generateTaskId(
    JNIEnv *env,
    jclass,
    jbyteArray did,
    jbyteArray ptid,
    jint parent_task_counter) {
  UniqueIdFromJByteArray o1(env, did);
  ray::DriverID driver_id = *o1.PID;

  UniqueIdFromJByteArray o2(env, ptid);
  ray::TaskID parent_task_id = *o2.PID;

  ray::TaskID task_id =
      ray::GenerateTaskId(driver_id, parent_task_id, parent_task_counter);
  jbyteArray result = env->NewByteArray(sizeof(ray::TaskID));
  if (nullptr == result) {
    return nullptr;
  }
  env->SetByteArrayRegion(result, 0, sizeof(TaskID),
                          reinterpret_cast<jbyte *>(&task_id));

  return result;
}

#ifdef __cplusplus
}
#endif
