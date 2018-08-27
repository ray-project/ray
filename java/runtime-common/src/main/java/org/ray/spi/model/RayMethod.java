package org.ray.spi.model;

import java.lang.reflect.Method;
import org.ray.api.annotation.RayRemote;
import org.ray.api.UniqueID;
import org.ray.util.MethodId;

/**
 * method info.
 */
public class RayMethod {

  public final Method invokable;
  public final String fullName;
  public final RayRemote remoteAnnotation;
  private final UniqueID funcId;

  private RayMethod(Method m, RayRemote remoteAnnotation, UniqueID funcId) {
    this.invokable = m;
    this.remoteAnnotation = remoteAnnotation;
    this.funcId = funcId;
    fullName = m.getDeclaringClass().getName() + "." + m.getName();
  }

  public static RayMethod from(Method m, RayRemote parentRemoteAnnotation) {
    Class<?> clazz = m.getDeclaringClass();
    RayRemote remoteAnnotation = m.getAnnotation(RayRemote.class);
    MethodId mid = MethodId.fromMethod(m);
    UniqueID funcId = new UniqueID(mid.getSha1Hash());
    RayMethod method = new RayMethod(m,
        remoteAnnotation != null ? remoteAnnotation : parentRemoteAnnotation,
        funcId);
    return method;
  }

  @Override
  public String toString() {
    return fullName;
  }

  public UniqueID getFuncId() {
    return funcId;
  }
}
