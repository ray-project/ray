package io.ray.serve.replica;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.ray.serve.common.Constants;
import io.ray.serve.deployment.DeploymentWrapper;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ReflectUtil;
import io.ray.serve.util.SignatureUtil;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** default impl of the ServeCallableProvider */
public class DefaultCallable implements ServeCallableProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCallable.class);

  @Override
  public String type() {
    return Constants.CALLABLE_PROVIDER_DEFAULT;
  }

  @Override
  public Object buildCallable(DeploymentWrapper deploymentWrapper) {
    try {
      // Instantiate the object defined by deploymentDef.
      Class deploymentClass =
          Class.forName(
              deploymentWrapper.getDeploymentDef(),
              true,
              Optional.ofNullable(Thread.currentThread().getContextClassLoader())
                  .orElse(getClass().getClassLoader()));
      return ReflectUtil.getConstructor(deploymentClass, deploymentWrapper.getInitArgs())
          .newInstance(deploymentWrapper.getInitArgs());
    } catch (Throwable e) {
      String errMsg =
          LogUtil.format(
              "Failed to initialize callable of deployment {}", deploymentWrapper.getName());
      LOGGER.error(errMsg, e);
      throw new RayServeException(errMsg, e);
    }
  }

  @Override
  public Map<String, Pair<Method, Object>> getSignatures(Object callable) {
    Class deploymentClass = callable.getClass();
    Map<String, Pair<Method, Object>> signatures = Maps.newHashMap();
    List<Method> methods = Lists.newArrayList();
    methods.addAll(Arrays.asList(deploymentClass.getDeclaredMethods()));
    Class clz = deploymentClass.getSuperclass();
    while (clz != null && clz != Object.class) {
      methods.addAll(Arrays.asList(clz.getDeclaredMethods()));
      clz = clz.getSuperclass();
    }
    for (Class baseInterface : deploymentClass.getInterfaces()) {
      for (Method method : baseInterface.getDeclaredMethods()) {
        if (method.isDefault()) {
          methods.add(method);
        }
      }
    }
    for (Method m : Lists.reverse(methods)) {
      m.setAccessible(true);
      String signature = SignatureUtil.getSignature(m);
      signatures.put(signature, ImmutablePair.of(m, callable));
    }
    return signatures;
  }
}
