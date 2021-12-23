package io.ray.serialization.serializers;

import io.ray.serialization.Fury;
import io.ray.serialization.io.FuryObjectInput;
import io.ray.serialization.io.FuryObjectOutput;
import io.ray.serialization.util.CommonUtil;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.Platform;
import java.io.Externalizable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ExternalizableSerializer<T extends Externalizable> extends Serializer<T> {
  private Constructor<T> constructor;
  private FuryObjectInput objectInput;
  private FuryObjectOutput objectOutput;

  public ExternalizableSerializer(Fury fury, Class<T> cls) {
    super(fury, cls);
    try {
      constructor = cls.getConstructor();
    } catch (NoSuchMethodException e) {
      CommonUtil.ignore(e);
    }

    objectInput = new FuryObjectInput(fury, null);
    objectOutput = new FuryObjectOutput(fury, null);
  }

  @Override
  public void write(Fury fury, MemoryBuffer buffer, T value) {
    objectOutput.setBuffer(buffer);
    try {
      value.writeExternal(objectOutput);
    } catch (IOException e) {
      Platform.throwException(e);
    }
  }

  @Override
  public T read(Fury fury, MemoryBuffer buffer, Class<T> type) {
    T t;
    if (constructor != null) {
      try {
        t = constructor.newInstance();
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    } else {
      t = Platform.newInstance(type);
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
