package io.ray.runtime.serialization.serializers;

import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.io.Platform;
import io.ray.runtime.io.SerdeObjectInput;
import io.ray.runtime.io.SerdeObjectOutput;
import io.ray.runtime.serialization.RaySerde;
import io.ray.runtime.serialization.Serializer;
import java.io.Externalizable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ExternalizableSerializer<T extends Externalizable> extends Serializer<T> {
  private Constructor<T> constructor;
  private final SerdeObjectInput objectInput;
  private final SerdeObjectOutput objectOutput;

  public ExternalizableSerializer(RaySerde raySerDe, Class<T> cls) {
    super(raySerDe, cls);
    try {
      constructor = cls.getConstructor();
    } catch (NoSuchMethodException e) {
      constructor = null;
    }
    objectInput = new SerdeObjectInput(raySerDe, null);
    objectOutput = new SerdeObjectOutput(raySerDe, null);
  }

  @Override
  public void write(MemoryBuffer buffer, T value) {
    objectOutput.setBuffer(buffer);
    try {
      value.writeExternal(objectOutput);
    } catch (IOException e) {
      Platform.throwException(e);
    }
  }

  @Override
  public T read(MemoryBuffer buffer) {
    T t;
    if (constructor != null) {
      try {
        t = constructor.newInstance();
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    } else {
      t = Platform.newInstance(cls);
    }
    objectInput.setBuffer(buffer);
    try {
      t.readExternal(objectInput);
    } catch (IOException | ClassNotFoundException e) {
      Platform.throwException(e);
    }
    return t;
  }
}
