package io.ray.serialization.serializers;

import io.ray.serialization.Fury;
import io.ray.serialization.util.CommonUtil;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.Platform;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public abstract class Serializer<T> {
  protected final Fury fury;
  protected final Class<T> cls;

  public abstract void write(Fury fury, MemoryBuffer buffer, T value);

  public abstract T read(Fury fury, MemoryBuffer buffer, Class<T> type);

  public Serializer(Fury fury, Class<T> cls) {
    this.fury = fury;
    this.cls = cls;
  }

  /**
   * Serializer sub class must have a constructor which take parameters of type {@link Fury} and
   * {@link Class}, or {@link Fury} or {@link Class} or no-arg constructor
   */
  public static Serializer newSerializer(
      Fury fury, Class type, Class<? extends Serializer> serializerClass) {
    try {
      try {
        Constructor<? extends Serializer> ctr =
            serializerClass.getConstructor(Fury.class, Class.class);
        ctr.setAccessible(true);
        return ctr.newInstance(fury, type);
      } catch (NoSuchMethodException e) {
        CommonUtil.ignore(e);
      }
      try {
        Constructor<? extends Serializer> ctr = serializerClass.getConstructor(Fury.class);
        ctr.setAccessible(true);
        return ctr.newInstance(fury);
      } catch (NoSuchMethodException e) {
        CommonUtil.ignore(e);
      }
      try {
        Constructor<? extends Serializer> ctr = serializerClass.getConstructor(Class.class);
        ctr.setAccessible(true);
        return ctr.newInstance(type);
      } catch (NoSuchMethodException e) {
        CommonUtil.ignore(e);
      }
      return serializerClass.newInstance();
    } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
      Platform.throwException(e);
    }
    throw new IllegalStateException("unreachable");
  }
}
