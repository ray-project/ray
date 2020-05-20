package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;


public class GlobalStateAccessor {
  private long globalStateAccessorNativePtr = 0;

  public GlobalStateAccessor(String redisAddress, String redisPassword) {
    globalStateAccessorNativePtr = nativeCreateGlobalStateAccessor(redisAddress, redisPassword);
    Preconditions.checkState(globalStateAccessorNativePtr != 0,
        "Global state accessor native pointr is non-nullptr");
  }

  public boolean connect() {
    return this.nativeConnect(globalStateAccessorNativePtr);
  }

  public void disconnect() {
    this.nativeDisConnect(globalStateAccessorNativePtr);
  }

  public List<byte[]> getAllJobInfo() {
    return this.nativeGetAllJobInfo(globalStateAccessorNativePtr);
  }

  public List<byte[]> getAllNodeInfo() {
    return this.nativeGetAllNodeInfo(globalStateAccessorNativePtr);
  }

  public void destroyGlobalStateAccessor() {
    this.nativeDestroyGlobalStateAccessor(globalStateAccessorNativePtr);
    globalStateAccessorNativePtr = 0;
  }

  private native long nativeCreateGlobalStateAccessor(String redisAddress, String redisPassword);

  private native void nativeDestroyGlobalStateAccessor(long nativePtr);

  private native boolean nativeConnect(long nativePtr);

  private native void nativeDisConnect(long nativePtr);

  private native List<byte[]> nativeGetAllJobInfo(long nativePtr);

  private native List<byte[]> nativeGetAllNodeInfo(long nativePtr);
}
