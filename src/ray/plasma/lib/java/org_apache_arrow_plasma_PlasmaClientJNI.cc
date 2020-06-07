// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "plasma/lib/java/org_apache_arrow_plasma_PlasmaClientJNI.h"

#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "arrow/util/logging.h"

#include "plasma/client.h"

constexpr jsize OBJECT_ID_SIZE = sizeof(plasma::ObjectID) / sizeof(jbyte);

inline void jbyteArray_to_object_id(JNIEnv* env, jbyteArray a, plasma::ObjectID* oid) {
  env->GetByteArrayRegion(a, 0, OBJECT_ID_SIZE, reinterpret_cast<jbyte*>(oid));
}

inline void object_id_to_jbyteArray(JNIEnv* env, jbyteArray a, plasma::ObjectID* oid) {
  env->SetByteArrayRegion(a, 0, OBJECT_ID_SIZE, reinterpret_cast<jbyte*>(oid));
}

inline void throw_exception_if_not_OK(JNIEnv* env, const arrow::Status& status) {
  if (!status.ok()) {
    jclass Exception =
        env->FindClass("org/apache/arrow/plasma/exceptions/PlasmaClientException");
    env->ThrowNew(Exception, status.message().c_str());
  }
}

class JByteArrayGetter {
 private:
  JNIEnv* _env;
  jbyteArray _a;
  jbyte* bp;

 public:
  JByteArrayGetter(JNIEnv* env, jbyteArray a, jbyte** out) {
    _env = env;
    _a = a;

    bp = _env->GetByteArrayElements(_a, nullptr);
    *out = bp;
  }

  ~JByteArrayGetter() { _env->ReleaseByteArrayElements(_a, bp, 0); }
};

JNIEXPORT jlong JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_connect(
    JNIEnv* env, jclass cls, jstring store_socket_name, jstring manager_socket_name,
    jint release_delay) {
  const char* s_name = env->GetStringUTFChars(store_socket_name, nullptr);
  const char* m_name = env->GetStringUTFChars(manager_socket_name, nullptr);

  plasma::PlasmaClient* client = new plasma::PlasmaClient();
  throw_exception_if_not_OK(env, client->Connect(s_name, m_name, release_delay));

  env->ReleaseStringUTFChars(store_socket_name, s_name);
  env->ReleaseStringUTFChars(manager_socket_name, m_name);
  return reinterpret_cast<int64_t>(client);
}

JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_disconnect(
    JNIEnv* env, jclass cls, jlong conn) {
  plasma::PlasmaClient* client = reinterpret_cast<plasma::PlasmaClient*>(conn);

  throw_exception_if_not_OK(env, client->Disconnect());
  delete client;
  return;
}

JNIEXPORT jobject JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_create(
    JNIEnv* env, jclass cls, jlong conn, jbyteArray object_id, jint size,
    jbyteArray metadata) {
  plasma::PlasmaClient* client = reinterpret_cast<plasma::PlasmaClient*>(conn);
  plasma::ObjectID oid;
  jbyteArray_to_object_id(env, object_id, &oid);

  // prepare metadata buffer
  uint8_t* md = nullptr;
  jsize md_size = 0;
  std::unique_ptr<JByteArrayGetter> md_getter;
  if (metadata != nullptr) {
    md_size = env->GetArrayLength(metadata);
  }
  if (md_size > 0) {
    md_getter.reset(new JByteArrayGetter(env, metadata, reinterpret_cast<jbyte**>(&md)));
  }

  std::shared_ptr<Buffer> data;
  Status s = client->Create(oid, size, md, md_size, &data);
  if (plasma::IsPlasmaObjectExists(s)) {
    jclass exceptionClass =
        env->FindClass("org/apache/arrow/plasma/exceptions/DuplicateObjectException");
    env->ThrowNew(exceptionClass, oid.hex().c_str());
    return nullptr;
  }
  if (plasma::IsPlasmaStoreFull(s)) {
    jclass exceptionClass =
        env->FindClass("org/apache/arrow/plasma/exceptions/PlasmaOutOfMemoryException");
    env->ThrowNew(exceptionClass, "");
    return nullptr;
  }
  throw_exception_if_not_OK(env, s);

  return env->NewDirectByteBuffer(data->mutable_data(), size);
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_hash(
    JNIEnv* env, jclass cls, jlong conn, jbyteArray object_id) {
  plasma::PlasmaClient* client = reinterpret_cast<plasma::PlasmaClient*>(conn);
  plasma::ObjectID oid;
  jbyteArray_to_object_id(env, object_id, &oid);

  unsigned char digest[plasma::kDigestSize];
  bool success = client->Hash(oid, digest).ok();

  if (success) {
    jbyteArray ret = env->NewByteArray(plasma::kDigestSize);
    env->SetByteArrayRegion(ret, 0, plasma::kDigestSize,
                            reinterpret_cast<jbyte*>(digest));
    return ret;
  } else {
    return nullptr;
  }
}

JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_seal(
    JNIEnv* env, jclass cls, jlong conn, jbyteArray object_id) {
  plasma::PlasmaClient* client = reinterpret_cast<plasma::PlasmaClient*>(conn);
  plasma::ObjectID oid;
  jbyteArray_to_object_id(env, object_id, &oid);

  throw_exception_if_not_OK(env, client->Seal(oid));
}

JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_release(
    JNIEnv* env, jclass cls, jlong conn, jbyteArray object_id) {
  plasma::PlasmaClient* client = reinterpret_cast<plasma::PlasmaClient*>(conn);
  plasma::ObjectID oid;
  jbyteArray_to_object_id(env, object_id, &oid);

  throw_exception_if_not_OK(env, client->Release(oid));
}

JNIEXPORT void JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_delete(
    JNIEnv* env, jclass cls, jlong conn, jbyteArray object_id) {
  plasma::PlasmaClient* client = reinterpret_cast<plasma::PlasmaClient*>(conn);
  plasma::ObjectID oid;
  jbyteArray_to_object_id(env, object_id, &oid);

  throw_exception_if_not_OK(env, client->Delete(oid));
}

JNIEXPORT jobjectArray JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_get(
    JNIEnv* env, jclass cls, jlong conn, jobjectArray object_ids, jint timeout_ms) {
  plasma::PlasmaClient* client = reinterpret_cast<plasma::PlasmaClient*>(conn);

  jsize num_oids = env->GetArrayLength(object_ids);
  std::vector<plasma::ObjectID> oids(num_oids);
  std::vector<plasma::ObjectBuffer> obufs(num_oids);
  for (int i = 0; i < num_oids; ++i) {
    jbyteArray_to_object_id(
        env, reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(object_ids, i)),
        &oids[i]);
  }
  // TODO: may be blocked. consider to add the thread support
  throw_exception_if_not_OK(env,
                            client->Get(oids.data(), num_oids, timeout_ms, obufs.data()));

  jclass clsByteBuffer = env->FindClass("java/nio/ByteBuffer");
  jclass clsByteBufferArray = env->FindClass("[Ljava/nio/ByteBuffer;");

  jobjectArray ret = env->NewObjectArray(num_oids, clsByteBufferArray, nullptr);
  jobjectArray o = nullptr;
  jobject dataBuf, metadataBuf;
  for (int i = 0; i < num_oids; ++i) {
    o = env->NewObjectArray(2, clsByteBuffer, nullptr);
    if (obufs[i].data && obufs[i].data->size() != -1) {
      dataBuf = env->NewDirectByteBuffer(const_cast<uint8_t*>(obufs[i].data->data()),
                                         obufs[i].data->size());
      if (obufs[i].metadata && obufs[i].metadata->size() > 0) {
        metadataBuf = env->NewDirectByteBuffer(
            const_cast<uint8_t*>(obufs[i].metadata->data()), obufs[i].metadata->size());
      } else {
        metadataBuf = nullptr;
      }
    } else {
      dataBuf = nullptr;
      metadataBuf = nullptr;
    }

    env->SetObjectArrayElement(o, 0, dataBuf);
    env->SetObjectArrayElement(o, 1, metadataBuf);
    env->SetObjectArrayElement(ret, i, o);
  }
  return ret;
}

JNIEXPORT jboolean JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_contains(
    JNIEnv* env, jclass cls, jlong conn, jbyteArray object_id) {
  plasma::PlasmaClient* client = reinterpret_cast<plasma::PlasmaClient*>(conn);
  plasma::ObjectID oid;
  jbyteArray_to_object_id(env, object_id, &oid);

  bool has_object;
  throw_exception_if_not_OK(env, client->Contains(oid, &has_object));

  return has_object;
}

JNIEXPORT jlong JNICALL Java_org_apache_arrow_plasma_PlasmaClientJNI_evict(
    JNIEnv* env, jclass cls, jlong conn, jlong num_bytes) {
  plasma::PlasmaClient* client = reinterpret_cast<plasma::PlasmaClient*>(conn);

  int64_t evicted_bytes;
  throw_exception_if_not_OK(
      env, client->Evict(static_cast<int64_t>(num_bytes), evicted_bytes));

  return static_cast<jlong>(evicted_bytes);
}
