package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import io.ray.api.id.ActorId;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.id.UniqueId;
import java.util.List;

/** `GlobalStateAccessor` is used for accessing information from GCS. */
public class GlobalStateAccessor {
  // NOTE(lingxuan.zlx): this is a singleton, it can not be changed during a Ray session.
  // Native pointer to the C++ GcsStateAccessor.
  private Long globalStateAccessorNativePointer = 0L;
  private static GlobalStateAccessor globalStateAccessor;

  public static synchronized GlobalStateAccessor getInstance(
      String redisAddress, String redisPassword) {
    if (null == globalStateAccessor) {
      globalStateAccessor = new GlobalStateAccessor(redisAddress, redisPassword);
    }
    return globalStateAccessor;
  }

  public static synchronized void destroyInstance() {
    if (null != globalStateAccessor) {
      globalStateAccessor.destroyGlobalStateAccessor();
      globalStateAccessor = null;
    }
  }

  private GlobalStateAccessor(String redisAddress, String redisPassword) {
    globalStateAccessorNativePointer = nativeCreateGlobalStateAccessor(redisAddress, redisPassword);
    validateGlobalStateAccessorPointer();
    connect();
  }

  private boolean connect() {
    return this.nativeConnect(globalStateAccessorNativePointer);
  }

  private void validateGlobalStateAccessorPointer() {
    Preconditions.checkState(
        globalStateAccessorNativePointer != 0,
        "Global state accessor native pointer must not be 0.");
  }

  /** Returns A list of job info with JobInfo protobuf schema. */
  public List<byte[]> getAllJobInfo() {
    // Fetch a job list with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetAllJobInfo(globalStateAccessorNativePointer);
    }
  }

  /** Returns A list of node info with GcsNodeInfo protobuf schema. */
  public List<byte[]> getAllNodeInfo() {
    // Fetch a node list with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetAllNodeInfo(globalStateAccessorNativePointer);
    }
  }

  /**
   * Get node resource info.
   *
   * @param nodeId node unique id.
   * @return A map of node resource info in protobuf schema.
   */
  public byte[] getNodeResourceInfo(UniqueId nodeId) {
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return nativeGetNodeResourceInfo(globalStateAccessorNativePointer, nodeId.getBytes());
    }
  }

  public byte[] getPlacementGroupInfo(PlacementGroupId placementGroupId) {
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return nativeGetPlacementGroupInfo(
          globalStateAccessorNativePointer, placementGroupId.getBytes());
    }
  }

  public byte[] getPlacementGroupInfo(String name, boolean global) {
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return nativeGetPlacementGroupInfoByName(globalStateAccessorNativePointer, name, global);
    }
  }

  public List<byte[]> getAllPlacementGroupInfo() {
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetAllPlacementGroupInfo(globalStateAccessorNativePointer);
    }
  }

  /** Returns A list of actor info with ActorInfo protobuf schema. */
  public List<byte[]> getAllActorInfo() {
    // Fetch a actor list with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetAllActorInfo(globalStateAccessorNativePointer);
    }
  }

  /** Returns An actor info with ActorInfo protobuf schema. */
  public byte[] getActorInfo(ActorId actorId) {
    // Fetch an actor with protobuf bytes format from GCS.
    synchronized (GlobalStateAccessor.class) {
      validateGlobalStateAccessorPointer();
      return this.nativeGetActorInfo(globalStateAccessorNativePointer, actorId.getBytes());
    }
  }

  private void destroyGlobalStateAccessor() {
    synchronized (GlobalStateAccessor.class) {
      if (0 == globalStateAccessorNativePointer) {
        return;
      }
      this.nativeDestroyGlobalStateAccessor(globalStateAccessorNativePointer);
      globalStateAccessorNativePointer = 0L;
    }
  }

  private native long nativeCreateGlobalStateAccessor(String redisAddress, String redisPassword);

  private native void nativeDestroyGlobalStateAccessor(long nativePtr);

  private native boolean nativeConnect(long nativePtr);

  private native List<byte[]> nativeGetAllJobInfo(long nativePtr);

  private native List<byte[]> nativeGetAllNodeInfo(long nativePtr);

  private native byte[] nativeGetNodeResourceInfo(long nativePtr, byte[] nodeId);

  private native List<byte[]> nativeGetAllActorInfo(long nativePtr);

  private native byte[] nativeGetActorInfo(long nativePtr, byte[] actorId);

  private native byte[] nativeGetPlacementGroupInfo(long nativePtr, byte[] placementGroupId);

  private native byte[] nativeGetPlacementGroupInfoByName(
      long nativePtr, String name, boolean global);

  private native List<byte[]> nativeGetAllPlacementGroupInfo(long nativePtr);
}
