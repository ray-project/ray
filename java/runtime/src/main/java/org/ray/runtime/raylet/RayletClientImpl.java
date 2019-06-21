package org.ray.runtime.raylet;

import org.ray.api.exception.RayException;
import org.ray.api.id.UniqueId;

public class RayletClientImpl implements RayletClient {

  /**
   * The pointer to c++'s core worker.
   */
  private long nativeCoreWorker;

  public RayletClientImpl(long nativeCoreWorker) {
    this.nativeCoreWorker = nativeCoreWorker;
  }

  @Override
  public UniqueId prepareCheckpoint(UniqueId actorId) {
    return new UniqueId(nativePrepareCheckpoint(nativeCoreWorker, actorId.getBytes()));
  }

  @Override
  public void notifyActorResumedFromCheckpoint(UniqueId actorId, UniqueId checkpointId) {
    nativeNotifyActorResumedFromCheckpoint(nativeCoreWorker, actorId.getBytes(), checkpointId.getBytes());
  }


  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    nativeSetResource(nativeCoreWorker, resourceName, capacity, nodeId.getBytes());
  }

  /// Native method declarations.
  ///
  /// If you change the signature of any native methods, please re-generate
  /// the C++ header file and update the C++ implementation accordingly:
  ///
  /// Suppose that $Dir is your ray root directory.
  /// 1) pushd $Dir/java/runtime/target/classes
  /// 2) javah -classpath .:$Dir/java/api/target/classes org.ray.runtime.raylet.RayletClientImpl
  /// 3) clang-format -i org_ray_runtime_raylet_RayletClientImpl.h
  /// 4) cp org_ray_runtime_raylet_RayletClientImpl.h $Dir/src/ray/raylet/lib/java/
  /// 5) vim $Dir/src/ray/raylet/lib/java/org_ray_runtime_raylet_RayletClientImpl.cc
  /// 6) popd

  private static native byte[] nativePrepareCheckpoint(long nativeCoreWorker, byte[] actorId);

  private static native void nativeNotifyActorResumedFromCheckpoint(long nativeCoreWorker, byte[] actorId,
                                                                    byte[] checkpointId);

  private static native void nativeSetResource(long nativeCoreWorker, String resourceName, double capacity,
                                               byte[] nodeId) throws RayException;
}
