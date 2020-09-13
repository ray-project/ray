#include "streaming_jni_common.h"

std::vector<ray::ObjectID> jarray_to_object_id_vec(JNIEnv *env, jobjectArray jarr) {
  int stringCount = env->GetArrayLength(jarr);
  std::vector<ray::ObjectID> object_id_vec;
  for (int i = 0; i < stringCount; i++) {
    auto jstr = (jbyteArray)(env->GetObjectArrayElement(jarr, i));
    UniqueIdFromJByteArray idFromJByteArray(env, jstr);
    object_id_vec.push_back(idFromJByteArray.PID);
  }
  return object_id_vec;
}

std::vector<ray::ActorID> jarray_to_actor_id_vec(JNIEnv *env, jobjectArray jarr) {
  int count = env->GetArrayLength(jarr);
  std::vector<ray::ActorID> actor_id_vec;
  for (int i = 0; i < count; i++) {
    auto bytes = (jbyteArray)(env->GetObjectArrayElement(jarr, i));
    std::string id_str(ray::ActorID::Size(), 0);
    env->GetByteArrayRegion(bytes, 0, ray::ActorID::Size(),
                            reinterpret_cast<jbyte *>(&id_str.front()));
    actor_id_vec.push_back(ActorID::FromBinary(id_str));
  }

  return actor_id_vec;
}

jint throwRuntimeException(JNIEnv *env, const char *message) {
  jclass exClass;
  char className[] = "java/lang/RuntimeException";
  exClass = env->FindClass(className);
  return env->ThrowNew(exClass, message);
}

jint throwChannelInitException(JNIEnv *env, const char *message,
                               const std::vector<ray::ObjectID> &abnormal_queues) {
  jclass array_list_class = env->FindClass("java/util/ArrayList");
  jmethodID array_list_constructor = env->GetMethodID(array_list_class, "<init>", "()V");
  jmethodID array_list_add =
      env->GetMethodID(array_list_class, "add", "(Ljava/lang/Object;)Z");
  jobject array_list = env->NewObject(array_list_class, array_list_constructor);

  for (auto &q_id : abnormal_queues) {
    jbyteArray jbyte_array = env->NewByteArray(kUniqueIDSize);
    env->SetByteArrayRegion(
        jbyte_array, 0, kUniqueIDSize,
        const_cast<jbyte *>(reinterpret_cast<const jbyte *>(q_id.Data())));
    env->CallBooleanMethod(array_list, array_list_add, jbyte_array);
  }

  jclass ex_class =
      env->FindClass("io/ray/streaming/runtime/transfer/ChannelInitException");
  jmethodID ex_constructor =
      env->GetMethodID(ex_class, "<init>", "(Ljava/lang/String;Ljava/util/List;)V");
  jstring message_jstr = env->NewStringUTF(message);
  jobject ex_obj = env->NewObject(ex_class, ex_constructor, message_jstr, array_list);
  env->DeleteLocalRef(message_jstr);
  return env->Throw((jthrowable)ex_obj);
}

jint throwChannelInterruptException(JNIEnv *env, const char *message) {
  jclass ex_class =
      env->FindClass("io/ray/streaming/runtime/transfer/ChannelInterruptException");
  return env->ThrowNew(ex_class, message);
}

jclass LoadClass(JNIEnv *env, const char *class_name) {
  jclass tempLocalClassRef = env->FindClass(class_name);
  jclass ret = (jclass)env->NewGlobalRef(tempLocalClassRef);
  STREAMING_CHECK(ret) << "Can't load Java class " << class_name;
  env->DeleteLocalRef(tempLocalClassRef);
  return ret;
}

template <typename NativeT>
void JavaListToNativeVector(JNIEnv *env, jobject java_list,
                            std::vector<NativeT> *native_vector,
                            std::function<NativeT(JNIEnv *, jobject)> element_converter) {
  jclass java_list_class = LoadClass(env, "java/util/List");
  jmethodID java_list_size = env->GetMethodID(java_list_class, "size", "()I");
  jmethodID java_list_get =
      env->GetMethodID(java_list_class, "get", "(I)Ljava/lang/Object;");
  int size = env->CallIntMethod(java_list, java_list_size);
  native_vector->clear();
  for (int i = 0; i < size; i++) {
    native_vector->emplace_back(
        element_converter(env, env->CallObjectMethod(java_list, java_list_get, (jint)i)));
  }
}

/// Convert a Java byte array to a C++ UniqueID.
template <typename ID>
inline ID JavaByteArrayToId(JNIEnv *env, const jbyteArray &bytes) {
  std::string id_str(ID::Size(), 0);
  env->GetByteArrayRegion(bytes, 0, ID::Size(),
                          reinterpret_cast<jbyte *>(&id_str.front()));
  return ID::FromBinary(id_str);
}

/// Convert a Java String to C++ std::string.
std::string JavaStringToNativeString(JNIEnv *env, jstring jstr) {
  const char *c_str = env->GetStringUTFChars(jstr, nullptr);
  std::string result(c_str);
  env->ReleaseStringUTFChars(static_cast<jstring>(jstr), c_str);
  return result;
}

/// Convert a Java List<String> to C++ std::vector<std::string>.
void JavaStringListToNativeStringVector(JNIEnv *env, jobject java_list,
                                        std::vector<std::string> *native_vector) {
  JavaListToNativeVector<std::string>(
      env, java_list, native_vector, [](JNIEnv *env, jobject jstr) {
        return JavaStringToNativeString(env, static_cast<jstring>(jstr));
      });
}

std::shared_ptr<ray::RayFunction> FunctionDescriptorToRayFunction(
    JNIEnv *env, jobject functionDescriptor) {
  jclass java_language_class = LoadClass(env, "io/ray/runtime/generated/Common$Language");
  jclass java_function_descriptor_class =
      LoadClass(env, "io/ray/runtime/functionmanager/FunctionDescriptor");
  jmethodID java_language_get_number =
      env->GetMethodID(java_language_class, "getNumber", "()I");
  jmethodID java_function_descriptor_get_language =
      env->GetMethodID(java_function_descriptor_class, "getLanguage",
                       "()Lio/ray/runtime/generated/Common$Language;");
  jobject java_language =
      env->CallObjectMethod(functionDescriptor, java_function_descriptor_get_language);
  auto language = static_cast<::Language>(
      env->CallIntMethod(java_language, java_language_get_number));
  std::vector<std::string> function_descriptor_list;
  jmethodID java_function_descriptor_to_list =
      env->GetMethodID(java_function_descriptor_class, "toList", "()Ljava/util/List;");
  JavaStringListToNativeStringVector(
      env, env->CallObjectMethod(functionDescriptor, java_function_descriptor_to_list),
      &function_descriptor_list);
  ray::FunctionDescriptor function_descriptor =
      ray::FunctionDescriptorBuilder::FromVector(language, function_descriptor_list);
  ray::RayFunction ray_function(language, function_descriptor);
  return std::make_shared<ray::RayFunction>(ray_function);
}

void ParseChannelInitParameters(
    JNIEnv *env, jobject param_obj,
    std::vector<ray::streaming::ChannelCreationParameter> &parameter_vec) {
  jclass java_streaming_queue_initial_parameters_class =
      LoadClass(env,
                "io/ray/streaming/runtime/transfer/"
                "ChannelCreationParametersBuilder");
  jmethodID java_streaming_queue_initial_parameters_getParameters_method =
      env->GetMethodID(java_streaming_queue_initial_parameters_class, "getParameters",
                       "()Ljava/util/List;");
  STREAMING_CHECK(java_streaming_queue_initial_parameters_getParameters_method !=
                  nullptr);
  jclass java_streaming_queue_initial_parameters_parameter_class =
      LoadClass(env,
                "io/ray/streaming/runtime/transfer/"
                "ChannelCreationParametersBuilder$Parameter");
  jmethodID java_getActorIdBytes_method = env->GetMethodID(
      java_streaming_queue_initial_parameters_parameter_class, "getActorIdBytes", "()[B");
  jmethodID java_getAsyncFunctionDescriptor_method =
      env->GetMethodID(java_streaming_queue_initial_parameters_parameter_class,
                       "getAsyncFunctionDescriptor",
                       "()Lio/ray/runtime/functionmanager/FunctionDescriptor;");
  jmethodID java_getSyncFunctionDescriptor_method =
      env->GetMethodID(java_streaming_queue_initial_parameters_parameter_class,
                       "getSyncFunctionDescriptor",
                       "()Lio/ray/runtime/functionmanager/FunctionDescriptor;");
  // Call getParameters method
  jobject parameter_list = env->CallObjectMethod(
      param_obj, java_streaming_queue_initial_parameters_getParameters_method);

  JavaListToNativeVector<ray::streaming::ChannelCreationParameter>(
      env, parameter_list, &parameter_vec,
      [java_getActorIdBytes_method, java_getAsyncFunctionDescriptor_method,
       java_getSyncFunctionDescriptor_method](JNIEnv *env, jobject jobject_parameter) {
        ray::streaming::ChannelCreationParameter native_parameter;
        jbyteArray jobject_actor_id_bytes = (jbyteArray)env->CallObjectMethod(
            jobject_parameter, java_getActorIdBytes_method);
        native_parameter.actor_id =
            JavaByteArrayToId<ray::ActorID>(env, jobject_actor_id_bytes);
        jobject jobject_async_func = env->CallObjectMethod(
            jobject_parameter, java_getAsyncFunctionDescriptor_method);
        native_parameter.async_function =
            FunctionDescriptorToRayFunction(env, jobject_async_func);
        jobject jobject_sync_func = env->CallObjectMethod(
            jobject_parameter, java_getSyncFunctionDescriptor_method);
        native_parameter.sync_function =
            FunctionDescriptorToRayFunction(env, jobject_sync_func);
        return native_parameter;
      });
}
