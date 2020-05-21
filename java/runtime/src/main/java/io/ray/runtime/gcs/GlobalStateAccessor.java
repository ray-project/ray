package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import java.util.List;


public class GlobalStateAccessor {
  private long globalStateAccessorNativePtr = 0;

  public GlobalStateAccessor(String redisAddress, String redisPassword) {
    globalStateAccessorNativePtr = nativeCreateGlobalStateAccessor(redisAddress, redisPassword);
    Preconditions.checkState(globalStateAccessorNativePtr != 0,
        "Global state accessor native pointr must not be 0.");
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
    this.nativeDestroyGlobalStateAccessor(globalStateAccessorNativePtr);
    globalStateAccessorNativePtr = 0;
  }

  private native long nativeCreateGlobalStateAccessor(String redisAddress, String redisPassword);

  private native void nativeDestroyGlobalStateAccessor(long nativePtr);

  private native boolean nativeConnect(long nativePtr);

  private native void nativeDisconnect(long nativePtr);

  private native List<byte[]> nativeGetAllJobInfo(long nativePtr);

  private native List<byte[]> nativeGetAllNodeInfo(long nativePtr);
}
