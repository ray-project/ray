package io.ray.serve.replica;

import io.ray.serve.common.Constants;
import io.ray.serve.generated.RequestWrapper;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** jersey impl of the ServeCallableProvider */
public class JerseyCallable extends DefaultCallable {
  private static final Logger LOGGER = LoggerFactory.getLogger(JerseyCallable.class);

  @Override
  public String type() {
    return Constants.CALLABLE_PROVIDER_JERSEY;
  }

  @Override
  public Map<String, Pair<Method, Object>> getSignatures(Object callable) {
    Map<String, Pair<Method, Object>> signatures = super.getSignatures(callable);
    JaxrsIngressInst jaxrsIngressInst = new JaxrsIngressInst(callable.getClass());
    try {
      signatures.put(
          Constants.HTTP_PROXY_SIGNATURE,
          Pair.of(
              jaxrsIngressInst.getClass().getDeclaredMethod("call", RequestWrapper.class),
              jaxrsIngressInst));
    } catch (NoSuchMethodException e) {
      LOGGER.error("call method in JaxrsIngressInst has not found.", e);
    }
    return signatures;
  }
}
