package org.ray.spi.model;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.UniqueId;
import org.ray.util.MethodId;

/**
 * method info.
 */
public class RayMethod {

  public final Executable invokable;
  public final String fullName;
  public final RayRemote remoteAnnotation;
  private final UniqueId funcId;

  private RayMethod(Executable e, RayRemote remoteAnnotation, UniqueId funcId) {
    this.invokable = e;
    this.remoteAnnotation = remoteAnnotation;
    this.funcId = funcId;
    fullName = e.getDeclaringClass().getName() + "." + e.getName();
  }

  public static RayMethod from(Executable e, RayRemote parentRemoteAnnotation) {
    RayRemote remoteAnnotation = e.getAnnotation(RayRemote.class);
    MethodId mid = MethodId.fromExecutable(e);
    UniqueId funcId = new UniqueId(mid.getSha1Hash());
    RayMethod method = new RayMethod(e,
        remoteAnnotation != null ? remoteAnnotation : parentRemoteAnnotation,
        funcId);
    return method;
  }

  public boolean isConstructor() {
    return invokable instanceof Constructor;
  }

  public Constructor<?> getConstructor() {
    return (Constructor<?>) invokable;
  }

  public Method getMethod() {
    return (Method) invokable;
  }

  @Override
  public String toString() {
    return fullName;
  }

  public UniqueId getFuncId() {
    return funcId;
  }
}
