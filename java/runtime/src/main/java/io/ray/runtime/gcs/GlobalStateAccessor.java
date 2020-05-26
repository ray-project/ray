package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import java.util.List;

/**
 * `GlobalStateAccessor` is used for accessing information from GCS.
 *
 **/
public class GlobalStateAccessor {
  // Native pointer to the C++ GcsStateAccessor.
  // NOTE(lingxuan.zlx): this is a singleton, it can not be changed during a Ray session.
  private static Long globalStateAccessorNativePointer = 0L;

  public GlobalStateAccessor(String redisAddress, String redisPassword) {
    synchronized (globalStateAccessorNativePointer) {
      if (0 == globalStateAccessorNativePointer) {
        globalStateAccessorNativePointer =
          nativeCreateGlobalStateAccessor(redisAddress, redisPassword);
      }
      Preconditions.checkState(globalStateAccessorNativePointer != 0,
          "Global state accessor native pointer must not be 0.");
    }
  }

  public boolean connect() {
    return this.nativeConnect(globalStateAccessorNativePointer);
  }

  public void disconnect() {
    this.nativeDisconnect(globalStateAccessorNativePointer);
  }

  /**
   * @return A list of job info with JobInfo protobuf schema.
   */
  public List<byte[]> getAllJobInfo() {
    // Fetch a job list with protobuf bytes format from GCS.
    synchronized (globalStateAccessorNativePointer) {
      Preconditions.checkState(globalStateAccessorNativePointer != 0);
      return this.nativeGetAllJobInfo(globalStateAccessorNativePointer);
    }
  }

  /**
   * @return A list of node info with GcsNodeInfo protobuf schema.
   */
  public List<byte[]> getAllNodeInfo() {
    // Fetch a node list with protobuf bytes format from GCS.
    synchronized (globalStateAccessorNativePointer) {
      Preconditions.checkState(globalStateAccessorNativePointer != 0);
      return this.nativeGetAllNodeInfo(globalStateAccessorNativePointer);
    }
  }

  public void destroyGlobalStateAccessor() {
    synchronized (globalStateAccessorNativePointer) {
      this.nativeDestroyGlobalStateAccessor(globalStateAccessorNativePointer);
      globalStateAccessorNativePointer = 0L;
    }
  }

  private native long nativeCreateGlobalStateAccessor(String redisAddress, String redisPassword);

  private native void nativeDestroyGlobalStateAccessor(long nativePtr);

  private native boolean nativeConnect(long nativePtr);

  private native void nativeDisconnect(long nativePtr);

  private native List<byte[]> nativeGetAllJobInfo(long nativePtr);

  private native List<byte[]> nativeGetAllNodeInfo(long nativePtr);
}
