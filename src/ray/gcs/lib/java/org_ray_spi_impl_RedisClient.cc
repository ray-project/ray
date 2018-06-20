#include <jni.h>

#include "org_ray_spi_impl_RedisClient.h"

#ifdef __cplusplus
extern "C" {
#endif

struct JstrWrapper {
  JstrWrapper(JNIEnv *env, jstring& str) : env_(env), str(str_) {
    nativeString_ = env->GetStringUTFChars(str_, JNI_FALSE);
  }

  const char* str() {
    return nativeString_;
  }

  ~EnvWrapper {
    env_->ReleaseStringUTFChars(str_, nativeString_);
  }
  
  char* nativeString_;
  JNIEnv *env_;
  jstring& str_;
};

JNIEXPORT jint JNICALL 
Java_org_ray_spi_impl_RedisClient_connect(JNIEnv *env, jclass, jstring addr, jint port) {
  JstrWrapper jstr(env, addr);
  auto context = redisConnect(jstr.str(), port);
  return reinterpret_cast<jlong>(client);
}

JNIEXPORT jbyteArray JNICALL 
Java_org_ray_spi_impl_RedisClient_execute_1command(JNIEnv *env, 
                                                   jclass, 
                                                   jint handle,
                                                   jstring command, 
                                                   jint type, 
                                                   jbyteArray) {
  auto context = reinterpret_cast<redisContext *>(c);
  
  JstrWrapper jstr(env, command);

  freeReplyObject(redisCommand(context, jstr.str(), type));
}

#ifdef __cplusplus
}
#endif