package io.ray.runtime.serialization.serializers;

import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.io.Platform;
import io.ray.runtime.serialization.RaySerde;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public abstract class Serializer<T> {
  protected final RaySerde raySerDe;
  protected final Class<T> cls;

  public abstract void write(RaySerde raySerDe, MemoryBuffer buffer, T value);

  public abstract T read(RaySerde raySerDe, MemoryBuffer buffer, Class<T> type);

  public Serializer(RaySerde raySerDe, Class<T> cls) {
    this.raySerDe = raySerDe;
    this.cls = cls;
  }

  /**
   * Serializer sub class must have a constructor which take parameters of type {@link RaySerde} and
   * {@link Class}, or {@link RaySerde} or {@link Class} or no-arg constructor.
   */
  public static Serializer newSerializer(
      RaySerde raySerDe, Class type, Class<? extends Serializer> serializerClass) {
    try {
      try {
        Constructor<? extends Serializer> ctr =
            serializerClass.getConstructor(RaySerde.class, Class.class);
        ctr.setAccessible(true);
        return ctr.newInstance(raySerDe, type);
      } catch (NoSuchMethodException ignored) {
        // ignore.
      }
      try {
        Constructor<? extends Serializer> ctr = serializerClass.getConstructor(RaySerde.class);
        ctr.setAccessible(true);
        return ctr.newInstance(raySerDe);
      } catch (NoSuchMethodException ignored) {
        // ignore.
      }
      try {
        Constructor<? extends Serializer> ctr = serializerClass.getConstructor(Class.class);
        ctr.setAccessible(true);
        return ctr.newInstance(type);
      } catch (NoSuchMethodException ignored) {
        // ignore.
      }
      return serializerClass.newInstance();
    } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
      Platform.throwException(e);
    }
    throw new IllegalStateException("unreachable");
  }
}
