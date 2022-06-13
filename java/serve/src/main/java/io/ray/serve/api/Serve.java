package io.ray.serve.api;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.serve.Constants;
import io.ray.serve.RayServeException;
import io.ray.serve.ReplicaContext;
import io.ray.serve.util.LogUtil;
import java.util.Optional;

/** Ray Serve global API. TODO: will be riched in the Java SDK/API PR. */
public class Serve {

  private static ReplicaContext INTERNAL_REPLICA_CONTEXT;

  private static Client GLOBAL_CLIENT;

  /**
   * Set replica information to global context.
   *
   * @param deploymentName deployment name
   * @param replicaTag replica tag
   * @param controllerName the controller actor's name
   * @param servableObject the servable object of the specified replica.
   */
  public static void setInternalReplicaContext(
      String deploymentName, String replicaTag, String controllerName, Object servableObject) {
    INTERNAL_REPLICA_CONTEXT =
        new ReplicaContext(deploymentName, replicaTag, controllerName, servableObject);
  }

  public static void setInternalReplicaContext(ReplicaContext replicaContext) {
    INTERNAL_REPLICA_CONTEXT = replicaContext;
  }

  /**
   * Get the global replica context.
   *
   * @return the replica context if it exists, or throw RayServeException.
   */
  public static ReplicaContext getReplicaContext() {
    if (INTERNAL_REPLICA_CONTEXT == null) {
      throw new RayServeException(
          "`Serve.getReplicaContext()` may only be called from within a Ray Serve deployment.");
    }
    return INTERNAL_REPLICA_CONTEXT;
  }

  public static Client getGlobalClient() {
    if (GLOBAL_CLIENT != null) {
      return GLOBAL_CLIENT;
    }
    synchronized (Client.class) {
      if (GLOBAL_CLIENT != null) {
        return GLOBAL_CLIENT;
      }
      return connect();
    }
  }

  public static void setGlobalClient(Client client) {
    GLOBAL_CLIENT = client;
  }

  public static Client connect() {

    if (!Ray.isInitialized()) {
      Ray.init();
    }

    String controllerName =
        INTERNAL_REPLICA_CONTEXT != null
            ? INTERNAL_REPLICA_CONTEXT.getInternalControllerName()
            : Constants.SERVE_CONTROLLER_NAME;

    Optional<BaseActorHandle> optional = Ray.getActor(controllerName, Constants.SERVE_NAMESPACE);
    Preconditions.checkState(
        optional.isPresent(),
        LogUtil.format(
            "There is no instance running on this Ray cluster. "
                + "Please call `serve.start(detached=True) to start one."));

    Client client = new Client(optional.get(), controllerName, true);
    setGlobalClient(client);
    return client;
  }
}
