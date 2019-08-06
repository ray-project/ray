#include "ray/core_worker/lib/java/org_ray_runtime_RayNativeRuntime.h"
#include "ray/core_worker/lib/java/jni_utils.h"
#include "ray/util/logging.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_ray_runtime_RayNativeRuntime
 * Method:    nativeStartRayLog
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_ray_runtime_RayNativeRuntime_nativeStartRayLog(
    JNIEnv *env, jclass, jstring logDir) {
  std::string log_dir = JavaStringToNativeString(env, logDir);
  ray::RayLog::StartRayLog("java_worker", ray::RayLogLevel::INFO, log_dir);
}

/*
 * Class:     org_ray_runtime_RayNativeRuntime
 * Method:    nativeShutdownRayLog
 * Signature: ()V
 */
JNIEXPORT void JNICALL
Java_org_ray_runtime_RayNativeRuntime_nativeShutdownRayLog(JNIEnv *, jclass) {
  ray::RayLog::ShutDownRayLog();
}

#ifdef __cplusplus
}
#endif
