package io.ray.serve.api;

import io.ray.serve.RayServeException;
import io.ray.serve.ReplicaContext;

/** Ray Serve global API. TODO: will be riched in the Java SDK/API PR. */
public class Serve {

  public static ReplicaContext INTERNAL_REPLICA_CONTEXT;

  /**
   * Set replica information to global context.
   *
   * @param backendTag backend tag
   * @param replicaTag replica tag
   * @param controllerName the controller actor's name
   * @param servableObject the servable object of the specified replica.
   */
  public static void setInternalReplicaContext(
      String backendTag, String replicaTag, String controllerName, Object servableObject) {
    // TODO singleton.
    INTERNAL_REPLICA_CONTEXT =
        new ReplicaContext(backendTag, replicaTag, controllerName, servableObject);
  }

  /**
   * Get the global replica context.
   *
   * @return the replica context if it exists, or throw RayServeException.
   */
  public static ReplicaContext getReplicaContext() {
    if (INTERNAL_REPLICA_CONTEXT == null) {
      throw new RayServeException(
          "`Serve.getReplicaContext()` may only be called from within a Ray Serve backend.");
    }
    return INTERNAL_REPLICA_CONTEXT;
  }
}
