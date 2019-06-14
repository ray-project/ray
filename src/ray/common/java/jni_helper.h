#ifndef RAY_COMMON_JAVA_JNI_HELPER_H
#define RAY_COMMON_JAVA_JNI_HELPER_H

#include <jni.h>
#include "ray/common/id.h"

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

template <typename ID>
class JByteArrayFromUniqueId {
 public:
  const jbyteArray &GetJByteArray() const { return jbytearray; }

  JByteArrayFromUniqueId(JNIEnv *env, const ID &id) {
    jbytearray = env->NewByteArray(ID::Size());
    env->SetByteArrayRegion(jbytearray, 0, ID::Size(),
                            reinterpret_cast<const jbyte *>(id.Data()));
  }

 private:
  jbyteArray jbytearray;
};

#endif  // RAY_COMMON_JAVA_JNI_HELPER_H
