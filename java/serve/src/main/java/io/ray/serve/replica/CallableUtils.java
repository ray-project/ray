package io.ray.serve.replica;

import io.ray.serve.common.Constants;
import io.ray.serve.deployment.DeploymentWrapper;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** callable utils */
public class CallableUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(CallableUtils.class);
  private static Map<String, ServeCallableProvider> callableProviderMap = null;

  public static ServeCallableProvider getProvider(String ingress) {
    if (null == callableProviderMap) {
      synchronized (CallableUtils.class) {
        if (null == callableProviderMap) {
          ServiceLoader<ServeCallableProvider> loader =
              ServiceLoader.load(
                  ServeCallableProvider.class, Thread.currentThread().getContextClassLoader());
          callableProviderMap = new HashMap<>();
          for (ServeCallableProvider provider : loader) {
            LOGGER.info("add ServeCallableProvider:type is {}.", provider.type());
            callableProviderMap.put(provider.type(), provider);
          }
        }
      }
    }
    if (StringUtils.isBlank(ingress)) {
      ingress = Constants.CALLABLE_PROVIDER_DEFAULT;
    }
    LOGGER.info("get ServeCallableProvider:type is {}.", ingress);
    ServeCallableProvider provider = callableProviderMap.get(ingress);
    if (null == provider) {
      LOGGER.error("can not find the ServeCallableProvider, type is {}.", ingress);
      throw new IllegalArgumentException(
          "can not find the ServeCallableProvider, type is " + ingress);
    }
    return provider;
  }

  public static Object makeCallable(DeploymentWrapper deploymentWrapper) {
    ServeCallableProvider provider =
        getProvider(deploymentWrapper.getDeploymentConfig().getIngress());
    return provider.buildCallable(deploymentWrapper);
  }

  public static Map<String, Pair<Method, Object>> getSignatures(String ingress, Object callable) {
    ServeCallableProvider provider = getProvider(ingress);
    return provider.getSignatures(callable);
  }
}
