package io.ray.serve;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/**
 * RayServeWrappedReplica.
 */
public class RayServeWrappedReplica {
  
  private RayServeReplica backend;

  @SuppressWarnings({"rawtypes", "unchecked"})
  public RayServeWrappedReplica(String backendTag, String replicaTag, String backendDef,
      Object[] initArgs, BackendConfig backendConfig, String controllerName)
      throws ClassNotFoundException, NoSuchMethodException, SecurityException,
      InstantiationException, IllegalAccessException, IllegalArgumentException,
      InvocationTargetException {

    Class backendClass = Class.forName(backendDef);
    
    Object callable = null;
    Class[] parameterTypes = null;
    if (initArgs != null && initArgs.length > 0) {
      parameterTypes = new Class[initArgs.length];
      for (int i = 0; i < initArgs.length; i++) {
        parameterTypes[i] = initArgs[i].getClass();
      }
    }
    callable = backendClass.getConstructor(parameterTypes).newInstance(initArgs);

    // TODO set_internal_replica_context. support in Java SDK's PR.

    Preconditions.checkArgument(StringUtils.isNotBlank(controllerName),
        "Must provide a valid controllerName");

    Optional<BaseActorHandle> optional = Ray.getActor(controllerName);
    Preconditions.checkState(optional.isPresent(), "Controller does not exist");

    backend = new RayServeReplica(callable, backendConfig, optional.get());

  }

  public void handle_request(RequestMetadata requestMetadata, List<Object> requestArgs) {
    backend.handle_request(new Query(requestArgs, requestMetadata));
  }

  public void ready() {
    return;
  }

  public void drain_pending_queries() {
    backend.drain_pending_queries();
  }

}
