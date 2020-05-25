package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import java.util.List;

/**
 * `GlobalStateAccessor` is used for accessing information from GCS.
 *
 **/
public class GlobalStateAccessor {
  // NOTE(lingxuan.zlx): Native pointer is singleton in gcs state accessor, which means it can not
  // be changed in cluster when redis or other storage accessor is fixed.
  private static Long globalStateAccessorNativePtr;

  public GlobalStateAccessor(String redisAddress, String redisPassword) {
    synchronized (globalStateAccessorNativePtr) {
      if (0 == globalStateAccessorNativePtr) {
        globalStateAccessorNativePtr = nativeCreateGlobalStateAccessor(redisAddress, redisPassword);
      }
      Preconditions.checkState(globalStateAccessorNativePtr != 0,
          "Global state accessor native pointer must not be 0.");
    }
  }

  public boolean connect() {
    return this.nativeConnect(globalStateAccessorNativePtr);
  }

  public void disconnect() {
    this.nativeDisconnect(globalStateAccessorNativePtr);
  }

  /**
   * @return A list of job info with JobInfo protobuf schema.
   */
  public List<byte[]> getAllJobInfo() {
    // Fetch a job list with protobuf bytes format from GCS.
    return this.nativeGetAllJobInfo(globalStateAccessorNativePtr);
  }

  /**
   * @return A list of node info with GcsNodeInfo protobuf schema.
   */
  public List<byte[]> getAllNodeInfo() {
    // Fetch a node list with protobuf bytes format from GCS.
    return this.nativeGetAllNodeInfo(globalStateAccessorNativePtr);
  }

  public void destroyGlobalStateAccessor() {
    synchronized (globalStateAccessorNativePtr) {
      this.nativeDestroyGlobalStateAccessor(globalStateAccessorNativePtr);
      globalStateAccessorNativePtr = 0L;
    }
  }

  private native long nativeCreateGlobalStateAccessor(String redisAddress, String redisPassword);

  private native void nativeDestroyGlobalStateAccessor(long nativePtr);

  private native boolean nativeConnect(long nativePtr);

  private native void nativeDisconnect(long nativePtr);

  private native List<byte[]> nativeGetAllJobInfo(long nativePtr);

  private native List<byte[]> nativeGetAllNodeInfo(long nativePtr);
}
