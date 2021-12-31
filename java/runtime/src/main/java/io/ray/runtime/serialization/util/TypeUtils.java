package io.ray.runtime.serialization.util;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@SuppressWarnings("UnstableApiUsage")
public class TypeUtils {
  public static Type ITERATOR_RETURN_TYPE;
  public static Type NEXT_RETURN_TYPE;
  public static Type KEY_SET_RETURN_TYPE;
  public static Type VALUES_RETURN_TYPE;

  static {
    try {
      ITERATOR_RETURN_TYPE = Iterable.class.getMethod("iterator").getGenericReturnType();
      NEXT_RETURN_TYPE = Iterator.class.getMethod("next").getGenericReturnType();
      KEY_SET_RETURN_TYPE = Map.class.getMethod("keySet").getGenericReturnType();
      VALUES_RETURN_TYPE = Map.class.getMethod("values").getGenericReturnType();
    } catch (NoSuchMethodException e) {
      throw new Error(e); // should be impossible
    }
  }

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

  /** Returns element type of iterable. */
  public static TypeToken<?> getElementType(TypeToken<?> typeToken) {
    @SuppressWarnings("unchecked")
    TypeToken<?> supertype =
        ((TypeToken<? extends Iterable<?>>) typeToken).getSupertype(Iterable.class);
    return supertype.resolveType(ITERATOR_RETURN_TYPE).resolveType(NEXT_RETURN_TYPE);
  }

  /** Returns key/value type of map. */
  public static Tuple2<TypeToken<?>, TypeToken<?>> getMapKeyValueType(TypeToken<?> typeToken) {
    @SuppressWarnings("unchecked")
    TypeToken<?> supertype = ((TypeToken<? extends Map<?, ?>>) typeToken).getSupertype(Map.class);
    TypeToken<?> keyType = getElementType(supertype.resolveType(KEY_SET_RETURN_TYPE));
    TypeToken<?> valueType = getElementType(supertype.resolveType(VALUES_RETURN_TYPE));
    return Tuple2.of(keyType, valueType);
  }
}
