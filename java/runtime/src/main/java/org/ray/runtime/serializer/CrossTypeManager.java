package org.ray.runtime.serializer;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CrossTypeManager {
  public static final int TYPE_ID_ACTOR_HANDLE = 1;

  public static final String KEY_CROSS_TYPE_ID = "crossTypeId";
  public static final String KEY_TO_CROSS_DATA = "toCrossData";
  public static final String KEY_FROM_CROSS_DATA = "fromCrossData";

  private static ConcurrentMap<Integer, Class<?>> typeTables = new ConcurrentHashMap<>();

  public static void register(Class<?> type) {
    try {
      Method crossTypeId = type.getDeclaredMethod(KEY_CROSS_TYPE_ID);
      crossTypeId.setAccessible(true);
      Integer typeId = (Integer) crossTypeId.invoke(null);
      type.getDeclaredMethod(KEY_TO_CROSS_DATA);
      type.getDeclaredMethod(KEY_FROM_CROSS_DATA, Object[].class);
      typeTables.put(typeId, type);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public static Class<?> get(Integer typeId) {
    return typeTables.get(typeId);
  }
}
