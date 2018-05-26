package org.ray.util;

import java.lang.reflect.InvocationTargetException;

public class ObjectUtil {

  public static <T> T newObject(Class<T> cls) {
    try {
      return cls.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static boolean[] toBooleanArray(Object[] vs) {
    boolean[] nvs = new boolean[vs.length];
    for (int i = 0; i < vs.length; i++) {
      nvs[i] = (boolean) vs[i];
    }
    return nvs;
  }

}
