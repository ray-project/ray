package io.ray.serve.replica;

import io.ray.serve.deployment.DeploymentWrapper;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

/** serve callable provider */
public interface ServeCallableProvider {
  /**
   * Get Callable type
   *
   * @return Callable type
   */
  String type();

  /**
   * generate a Callable instance
   *
   * @param deploymentWrapper deployment info and config
   * @return Callable instance
   */
  Object buildCallable(DeploymentWrapper deploymentWrapper);

  /**
   * get the signature of callable
   *
   * @param callable Callable instance
   * @return
   */
  Map<String, Pair<Method, Object>> getSignatures(Object callable);
}
