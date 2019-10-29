#include "com_alipay_streaming_runtime_memory_memorypool_jni_DirectMemory.h"
#include "streaming_logging.h"
#include "streaming_utility.h"
#include "streaming_memory_pool.h"

JNIEXPORT jlong JNICALL Java_com_alipay_streaming_runtime_memory_memorypool_jni_DirectMemory_createMemoryPool
  (JNIEnv *env, jobject obj, jlong size) {
  ray::streaming::StreamingMemoryPool* pool = new ray::streaming::NginxMemoryPool(size) ;
  STREAMING_LOG(DEBUG) <<"poo addr => " << pool;
  return reinterpret_cast<jlong>(pool);
}

JNIEXPORT void JNICALL Java_com_alipay_streaming_runtime_memory_memorypool_jni_DirectMemory_releaseMemoryPool
(JNIEnv *env, jobject obj, jlong pool_ptr) {
  delete reinterpret_cast<ray::streaming::StreamingMemoryPool*>(pool_ptr);
}

JNIEXPORT jobject JNICALL Java_com_alipay_streaming_runtime_memory_memorypool_jni_DirectMemory_requestMemory
  (JNIEnv *env, jobject obj, jlong alloc_size, jlong pool_ptr) {
  jlong buff_ptr = Java_com_alipay_streaming_runtime_memory_memorypool_jni_DirectMemory_allocateMemory(
    env, obj, alloc_size, pool_ptr);
  if (buff_ptr) {
    return env->NewDirectByteBuffer(reinterpret_cast<uint8_t*>(buff_ptr), alloc_size);
  }
  return NULL;
}

JNIEXPORT jlong JNICALL Java_com_alipay_streaming_runtime_memory_memorypool_jni_DirectMemory_allocateMemory
  (JNIEnv *env, jobject obj, jlong alloc_size, jlong pool_ptr) {
  ray::streaming::StreamingMemoryPool* sp =
    reinterpret_cast<ray::streaming::StreamingMemoryPool*>(pool_ptr);
  unsigned char *buff = (unsigned char *) sp->RequestMemory(alloc_size);
  STREAMING_LOG(DEBUG) <<"poo addr => " << sp
                       << " alloc size => " << alloc_size
                       << " addr => " << reinterpret_cast<long>(buff);
  return reinterpret_cast<long>(buff);
}

JNIEXPORT void JNICALL Java_com_alipay_streaming_runtime_memory_memorypool_jni_DirectMemory_releaseMemory__Ljava_nio_ByteBuffer_2J
(JNIEnv *env, jobject obj, jobject buffer_byte_obj, jlong pool_ptr) {
  uint8_t *buff = (uint8_t*)env->GetDirectBufferAddress(buffer_byte_obj);
  Java_com_alipay_streaming_runtime_memory_memorypool_jni_DirectMemory_releaseMemory__JJ(
    env, obj, reinterpret_cast<jlong>(buff), pool_ptr);
}

JNIEXPORT void JNICALL Java_com_alipay_streaming_runtime_memory_memorypool_jni_DirectMemory_releaseMemory__JJ
(JNIEnv *env, jobject obj, jlong buff_ptr, jlong pool_ptr) {
  ray::streaming::StreamingMemoryPool* sp =
    reinterpret_cast<ray::streaming::StreamingMemoryPool*>(pool_ptr);
  uint8_t *buff = reinterpret_cast<uint8_t *>(buff_ptr);
  STREAMING_LOG(DEBUG) <<"poo addr => " << sp << " free addr => " << buff_ptr;
  sp->ReleaseMemory(buff);
}

JNIEXPORT void JNICALL Java_com_alipay_streaming_runtime_memory_memorypool_jni_DirectMemory_showMemoryBuffer
(JNIEnv *env, jobject obj, jobject buffer_byte_obj, jint size) {
  uint8_t *buff = (uint8_t*)env->GetDirectBufferAddress(buffer_byte_obj);
  STREAMING_LOG(INFO) <<" buffer => " << reinterpret_cast<long>(buff)
                      <<", content =>" << ray::streaming::StreamingUtility::Byte2hex(buff, size);
}

