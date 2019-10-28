#include "streaming_jni_common.h"

jclass java_direct_buffer_class;
jfieldID java_direct_buffer_address;
jfieldID java_direct_buffer_capacity;

std::vector<ray::ObjectID>
jarray_to_plasma_object_id_vec(JNIEnv *env, jobjectArray jarr) {
  int stringCount = env->GetArrayLength(jarr);
  std::vector<ray::ObjectID> object_id_vec;
  for (int i = 0; i < stringCount; i++) {
    auto jstr = (jbyteArray) (env->GetObjectArrayElement(jarr, i));
    UniqueIdFromJByteArray idFromJByteArray(env, jstr);
    object_id_vec.push_back(idFromJByteArray.PID);
  }
   return object_id_vec;
}

jint throwRuntimeException(JNIEnv *env, const char *message) {
  jclass exClass;
  char className[] = "java/lang/RuntimeException";
  exClass = env->FindClass(className);
  return env->ThrowNew(exClass, message);
}

jint throwQueueInitException(JNIEnv *env, const char *message, const std::vector<ray::ObjectID> &abnormal_queues) {
  jclass array_list_class = env->FindClass("java/util/ArrayList");
  jmethodID array_list_constructor = env->GetMethodID(array_list_class, "<init>", "()V");
  jmethodID array_list_add = env->GetMethodID(array_list_class, "add", "(Ljava/lang/Object;)Z");
  jobject array_list = env->NewObject(array_list_class, array_list_constructor);

  for (auto &q_id : abnormal_queues) {
    jbyteArray jbyte_array = env->NewByteArray(plasma::kUniqueIDSize);
    env->SetByteArrayRegion(jbyte_array, 0, plasma::kUniqueIDSize, reinterpret_cast<const jbyte *>(q_id.Data()));
    env->CallBooleanMethod(array_list, array_list_add, jbyte_array);
  }

  jclass ex_class = env->FindClass("com/alipay/streaming/runtime/queue/impl/plasma/exception/QueueInitException");
  jmethodID ex_constructor = env->GetMethodID(ex_class, "<init>", "(Ljava/lang/String;Ljava/util/List;)V");
  jstring message_jstr = env->NewStringUTF(message);
  jobject ex_obj = env->NewObject(ex_class, ex_constructor, message_jstr, array_list);
  env->DeleteLocalRef(message_jstr);
  return env->Throw((jthrowable)ex_obj);
}

jint throwQueueInterruptException(JNIEnv *env, const char *message) {
  jclass ex_class = env->FindClass("com/alipay/streaming/runtime/queue/impl/plasma/exception/QueueInterruptException");
  return env->ThrowNew(ex_class, message);
}

jint JNI_OnLoad(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  if (vm->GetEnv(reinterpret_cast<void **>(&env), CURRENT_JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  java_direct_buffer_class = FindClass(env, "java/nio/DirectByteBuffer");
  java_direct_buffer_address = env->GetFieldID(java_direct_buffer_class, "address", "J");
  STREAMING_CHECK(java_direct_buffer_address != nullptr);
  java_direct_buffer_capacity = env->GetFieldID(java_direct_buffer_class, "capacity", "I");
  STREAMING_CHECK(java_direct_buffer_capacity != nullptr);

  return CURRENT_JNI_VERSION;
}