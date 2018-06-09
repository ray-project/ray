/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2018 All Rights Reserved.
 */
package org.ray.spi.model;

import com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.ray.api.RayRemote;
import org.ray.api.UniqueID;


public final class RayActorMethods {

  public final Class clazz;
  public final RayRemote remoteAnnotation;
  public final Map<UniqueID, RayMethod> functions;
  /**
   * the static function in Actor, call as task
   */
  public final Map<UniqueID, RayMethod> staticFunctions;

  private RayActorMethods(Class clazz, RayRemote remoteAnnotation,
      Map<UniqueID, RayMethod> functions, Map<UniqueID, RayMethod> staticFunctions) {
    this.clazz = clazz;
    this.remoteAnnotation = remoteAnnotation;
    this.functions = Collections.unmodifiableMap(new HashMap<>(functions));
    this.staticFunctions = Collections.unmodifiableMap(new HashMap<>(staticFunctions));
  }

  public static RayActorMethods formClass(String clazzName, ClassLoader classLoader) {
    try {
      Class clazz = Class.forName(clazzName, true, classLoader);
      RayRemote remoteAnnotation = (RayRemote) clazz.getAnnotation(RayRemote.class);
      Preconditions
          .checkNotNull(remoteAnnotation, "% must declare @RayRemote", clazzName);
      Method[] methods = clazz.getMethods();
      Map<UniqueID, RayMethod> functions = new HashMap<>(methods.length * 2);
      Map<UniqueID, RayMethod> staticFunctions = new HashMap<>(methods.length * 2);

      for (Method m : methods) {
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
    return String.format("RayActorMethods:%s, funcNum=%s, sfuncNum=%s", clazz, functions.size(),
        staticFunctions.size());
  }

}