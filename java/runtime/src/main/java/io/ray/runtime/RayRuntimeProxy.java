package io.ray.runtime;

import io.ray.api.exception.RayException;
import io.ray.api.runtime.RayRuntime;
import io.ray.runtime.config.RunMode;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Protect a ray runtime with context checks for all methods of {@link RayRuntime} (except {@link
 * RayRuntime#shutdown(boolean)}).
 */
public class RayRuntimeProxy implements InvocationHandler {

  /**
   * The original runtime.
   */
  private AbstractRayRuntime obj;

  private RayRuntimeProxy(AbstractRayRuntime obj) {
    this.obj = obj;
  }

  public AbstractRayRuntime getRuntimeObject() {
    return obj;
  }

  /**
   * Generate a new instance of {@link RayRuntimeInternal} with additional context check.
   */
  static RayRuntimeInternal newInstance(AbstractRayRuntime obj) {
    return (RayRuntimeInternal) java.lang.reflect.Proxy
        .newProxyInstance(obj.getClass().getClassLoader(), new Class<?>[]{RayRuntimeInternal.class},
            new RayRuntimeProxy(obj));
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if (isInterfaceMethod(method) && !method.getName().equals("shutdown") && !method.getName()
        .equals("setAsyncContext")) {
      checkIsContextSet();
    }
    try {
      return method.invoke(obj, args);
    } catch (InvocationTargetException e) {
      if (e.getCause() != null) {
        throw e.getCause();
      } else {
        throw e;
      }
    }
  }

  /**
   * Whether the method is defined in the {@link RayRuntime} interface.
   */
  private boolean isInterfaceMethod(Method method) {
    try {
      RayRuntime.class.getMethod(method.getName(), method.getParameterTypes());
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  /**
   * Check if thread context is set.
   * <p/>
   * This method should be invoked at the beginning of most public methods of {@link RayRuntime},
   * otherwise the native code might crash due to thread local core worker was not set. We check it
   * for {@link AbstractRayRuntime} instead of {@link RayNativeRuntime} because we want to catch the
   * error even if the application runs in {@link RunMode#SINGLE_PROCESS} mode.
   */
  private void checkIsContextSet() {
    if (!obj.isContextSet.get()) {
      throw new RayException(
          "`Ray.wrap***` is not called on the current thread."
              + " If you want to use Ray API in your own threads,"
              + " please wrap your executable with `Ray.wrap***`.");
    }
  }
}
