package io.ray.runtime.serialization.util;

import com.google.common.base.Preconditions;

public class TypeUtils {
  public static Tuple2<Class<?>, Integer> getArrayComponentInfo(Class<?> type) {
    Preconditions.checkArgument(type.isArray());
    Class<?> t = type;
    Class<?> innerType = type;
    int dimension = 0;
    while (t != null && t.isArray()) {
      dimension++;
      t = t.getComponentType();
      if (t != null) {
        innerType = t;
      }
    }
    return Tuple2.of(innerType, dimension);
  }
}
