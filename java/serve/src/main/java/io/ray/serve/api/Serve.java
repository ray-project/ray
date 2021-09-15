package io.ray.serve.api;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.serve.Constants;
import io.ray.serve.RayServeException;
import io.ray.serve.ReplicaContext;
import io.ray.serve.util.LogUtil;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/** Ray Serve global API. TODO: will be riched in the Java SDK/API PR. */
public class Serve {

  private static ReplicaContext INTERNAL_REPLICA_CONTEXT;

  private static Client GLOBAL_CLIENT;

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

  public static Client getGlobalClient() {
    return GLOBAL_CLIENT != null ? GLOBAL_CLIENT : connect(null); // TODO singleton
  }

  public static void setGlobalClient(Client client) {
    GLOBAL_CLIENT = client;
  }

  public static Client connect(String serviceName) {

    if (!Ray.isInitialized()) {
      Ray.init();
    }

    String controllerName =
        INTERNAL_REPLICA_CONTEXT != null
            ? INTERNAL_REPLICA_CONTEXT.getInternalControllerName()
            : StringUtils.isBlank(serviceName)
                ? Constants.SERVE_CONTROLLER_NAME
                : Constants.SERVE_CONTROLLER_NAME + "_" + serviceName;

    Optional<BaseActorHandle> optional = Ray.getActor(controllerName);
    Preconditions.checkState(
        optional.isPresent(),
        LogUtil.format(
            "There is no instance {} running on this Ray cluster. "
                + "Please call `serve.start(detached=True) to start one.",
            controllerName));

    return new Client(optional.get(), controllerName, true);
  }
}
