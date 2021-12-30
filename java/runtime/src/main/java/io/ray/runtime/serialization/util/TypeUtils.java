package io.ray.runtime.serialization.util;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;

public class TypeUtils {
  // sorted by size
  private static final List<Class<?>> sortedPrimitiveClasses =
      Arrays.asList(
          byte.class,
          boolean.class,
          char.class,
          short.class,
          int.class,
          long.class,
          float.class,
          double.class);

  public static boolean isPrimitive(Class<?> clz) {
    return sortedPrimitiveClasses.contains(clz);
  }

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
