package io.ray.streaming.runtime.serialization;

import io.ray.runtime.serializer.FstSerializer;

public class JavaSerializer implements Serializer {
  @Override
  public byte[] serialize(Object object) {
    return FstSerializer.encode(object);
  }

  @Override
  public <T> T deserialize(byte[] bytes) {
    return FstSerializer.decode(bytes);
  }
}
