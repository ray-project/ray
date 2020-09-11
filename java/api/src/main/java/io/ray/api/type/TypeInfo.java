package io.ray.api.type;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @param <T> Captures the actual type of {@code T}.
 */
public class TypeInfo<T> implements Serializable {
  public static final TypeInfo<ByteBuffer> BYTE_BUFFER_TYPE_INFO = new TypeInfo<>(ByteBuffer.class);

  private final Type type;

  public TypeInfo() {
    type = capture();
  }

  public TypeInfo(Type type) {
    this.type = type;
  }

  public TypeInfo(Class<T> type) {
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  /** Returns the captured type. */
  protected final java.lang.reflect.Type capture() {
    java.lang.reflect.Type superclass = getClass().getGenericSuperclass();
    if (!(superclass instanceof ParameterizedType)) {
      throw new RuntimeException(superclass + " isn't parameterized");
    }
    return ((ParameterizedType) superclass).getActualTypeArguments()[0];
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TypeInfo<?> typeInfo = (TypeInfo<?>) o;
    return Objects.equals(type, typeInfo.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
  }

  @Override
  public String toString() {
    return type.toString();
  }
}
