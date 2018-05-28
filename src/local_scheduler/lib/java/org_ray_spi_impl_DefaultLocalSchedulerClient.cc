#include <jni.h>

#include "local_scheduler/lib/java/org_ray_spi_impl_DefaultLocalSchedulerClient.h"
#include "local_scheduler_client.h"
#include "logging.h"

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
                                                         jlong numGpus) {
  // 	native private static long _init(String localSchedulerSocket,
  //     byte[] workerId, byte[] actorId, boolean isWorker, long numGpus);
  UniqueIdFromJByteArray worker_id(env, wid);
  const char *nativeString = env->GetStringUTFChars(sockName, JNI_FALSE);
  auto client =
      LocalSchedulerConnection_init(nativeString, *worker_id.PID, isWorker);
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
    jint sz) {
  // task -> TaskInfo (with FlatBuffer)
  // native private static void _submitTask(long client, /*Direct*/ByteBuffer
  // task);
  auto client = reinterpret_cast<LocalSchedulerConnection *>(c);
  TaskSpec *task =
      reinterpret_cast<char *>(env->GetDirectBufferAddress(buff)) + pos;
  std::vector<ObjectID> execution_dependencies;
  if (cursorId != nullptr) {
    UniqueIdFromJByteArray cursor_id(env, cursorId);
    execution_dependencies.push_back(*cursor_id.PID);
  }
  TaskExecutionSpec taskExecutionSpec =
      TaskExecutionSpec(execution_dependencies, task, sz);
  local_scheduler_submit(client, taskExecutionSpec);
}

/*
 * Class:     org_ray_spi_impl_DefaultLocalSchedulerClient
 * Method:    _getTaskTodo
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1getTaskTodo(JNIEnv *env,
                                                                jclass,
                                                                jlong c) {
  // native private static ByteBuffer _getTaskTodo(long client);
  auto client = reinterpret_cast<LocalSchedulerConnection *>(c);
  int64_t task_size = 0;

  // TODO: handle actor failure later
  TaskSpec *spec = local_scheduler_get_task(client, &task_size);

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
 * Method:    _reconstruct_object
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL
Java_org_ray_spi_impl_DefaultLocalSchedulerClient__1reconstruct_1object(
    JNIEnv *env,
    jclass,
    jlong c,
    jbyteArray oid) {
  // native private static void _reconstruct_object(long client, byte[]
  // objectId);
  UniqueIdFromJByteArray o(env, oid);
  auto client = reinterpret_cast<LocalSchedulerConnection *>(c);
  local_scheduler_reconstruct_object(client, *o.PID);
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

#ifdef __cplusplus
}
#endif
