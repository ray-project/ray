package org.ray.spi.model;

import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.UniqueId;


public final class RayTaskMethods {

  public final Class clazz;
  public final Map<UniqueId, RayMethod> functions;

  public RayTaskMethods(Class clazz,
      Map<UniqueId, RayMethod> functions) {
    this.clazz = clazz;
    this.functions = Collections.unmodifiableMap(new HashMap<>(functions));
  }

  public static RayTaskMethods fromClass(String clazzName, ClassLoader classLoader) {
    try {
      Class clazz = Class.forName(clazzName, true, classLoader);
      List<Executable> executables = new ArrayList<>();
      executables.addAll(Arrays.asList(clazz.getDeclaredMethods()));
      executables.addAll(Arrays.asList(clazz.getConstructors()));
      Map<UniqueId, RayMethod> functions = new HashMap<>(executables.size());

      for (Executable e : executables) {
//        if (!Modifier.isStatic(e.getModifiers())) {
//          continue;
//        }
        //task method only for static.
//        RayRemote remoteAnnotation = e.getAnnotation(RayRemote.class);
//        if (remoteAnnotation == null) {
//          continue;
//        }
        e.setAccessible(true);
        RayMethod rayMethod = RayMethod.from(e, null);
        functions.put(rayMethod.getFuncId(), rayMethod);
      }
      return new RayTaskMethods(clazz, functions);
    } catch (Exception e) {
      throw new RuntimeException("failed to get RayTaskMethods from " + clazzName, e);
    }
  }

  @Override
  public String toString() {
    return String
        .format("RayTaskMethods:%s, funcNum=%s:{%s}", clazz, functions.size(), functions.values());
  }

}