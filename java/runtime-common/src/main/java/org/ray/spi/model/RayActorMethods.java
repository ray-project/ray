package org.ray.spi.model;

import com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.UniqueId;


public final class RayActorMethods {

  public final Class clazz;
  public final RayRemote remoteAnnotation;
  public final Map<UniqueId, RayMethod> functions;
  /**
   * the static function in Actor, call as task.
   */
  public final Map<UniqueId, RayMethod> staticFunctions;

  private RayActorMethods(Class clazz, RayRemote remoteAnnotation,
      Map<UniqueId, RayMethod> functions, Map<UniqueId, RayMethod> staticFunctions) {
    this.clazz = clazz;
    this.remoteAnnotation = remoteAnnotation;
    this.functions = Collections.unmodifiableMap(new HashMap<>(functions));
    this.staticFunctions = Collections.unmodifiableMap(new HashMap<>(staticFunctions));
  }

  public static RayActorMethods fromClass(String clazzName, ClassLoader classLoader) {
    try {
      Class clazz = Class.forName(clazzName, true, classLoader);
      RayRemote remoteAnnotation = (RayRemote) clazz.getAnnotation(RayRemote.class);
      Preconditions
          .checkNotNull(remoteAnnotation, "%s must declare @RayRemote", clazzName);
      Method[] methods = clazz.getDeclaredMethods();
      Map<UniqueId, RayMethod> functions = new HashMap<>(methods.length * 2);
      Map<UniqueId, RayMethod> staticFunctions = new HashMap<>(methods.length * 2);

      for (Method m : methods) {
        if (!Modifier.isPublic(m.getModifiers())) {
          continue;
        }
        RayMethod rayMethod = RayMethod.from(m, remoteAnnotation);
        if (Modifier.isStatic(m.getModifiers())) {
          staticFunctions.put(rayMethod.getFuncId(), rayMethod);
        } else {
          functions.put(rayMethod.getFuncId(), rayMethod);
        }
      }
      return new RayActorMethods(clazz, remoteAnnotation, functions, staticFunctions);
    } catch (Exception e) {
      throw new RuntimeException("failed to get RayActorMethods from " + clazzName, e);
    }
  }

  @Override
  public String toString() {
    return String
        .format("RayActorMethods:%s, funcNum=%s:{%s}, sfuncNum=%s:{%s}", clazz, functions.size(),
            functions.values(),
            staticFunctions.size(), staticFunctions.values());
  }

}