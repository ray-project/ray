package io.ray.api.type;

import java.lang.reflect.ParameterizedType;

/**
 * @param <T> Captures the actual type of {@code T}.
 */
public abstract class Type<T> {

  /** Returns the captured type. */
  final java.lang.reflect.Type capture() {
    java.lang.reflect.Type superclass = getClass().getGenericSuperclass();
    if (!(superclass instanceof ParameterizedType)) {
      throw new RuntimeException(superclass + " isn't parameterized");
    }
    return ((ParameterizedType) superclass).getActualTypeArguments()[0];
  }

}
