package io.ray.serve;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.util.ReflectUtil;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/** Replica class wrapping the provided class. Note that Java function is not supported now. */
public class RayServeWrappedReplica {

  private RayServeReplica backend;

  @SuppressWarnings("rawtypes")
  public RayServeWrappedReplica(
      String backendTag,
      String replicaTag,
      String backendDef,
      Object[] initArgs,
      BackendConfig backendConfig,
      String controllerName)
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException {

    // Instantiate the object defined by backendDef.
    Class backendClass = Class.forName(backendDef);
    Object callable = ReflectUtil.getConstructor(backendClass, initArgs).newInstance(initArgs);

    // Get the controller by controllerName.
    Preconditions.checkArgument(
        StringUtils.isNotBlank(controllerName), "Must provide a valid controllerName");
    Optional<BaseActorHandle> optional = Ray.getActor(controllerName);
    Preconditions.checkState(optional.isPresent(), "Controller does not exist");

    // Set the controller name so that Serve.connect() in the user's backend code will connect to
    // the instance that this backend is running in.
    Serve.setInternalReplicaContext(backendTag, replicaTag, controllerName, callable);

    // Construct worker replica.
    backend = new RayServeReplica(callable, backendConfig, optional.get());
  }

  /**
   * The entry method to process the request.
   *
   * @param requestMetadata request metadata
   * @param requestArgs the input parameters of the specified method of the object defined by
   *     backendDef.
   * @return the result of request being processed
   */
  public Object handle_request(RequestMetadata requestMetadata, Object[] requestArgs) {
    return backend.handleRequest(new Query(requestArgs, requestMetadata));
  }

  /** Check whether this replica is ready or not. */
  public void ready() {
    return;
  }

  /** Wait until there is no request in processing. It is used for stopping replica gracefully. */
  public void drain_pending_queries() {
    backend.drainPendingQueries();
  }
}
