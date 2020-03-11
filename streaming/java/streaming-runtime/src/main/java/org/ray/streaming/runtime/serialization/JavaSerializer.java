package org.ray.streaming.runtime.serialization;

public class JavaSerializer implements Serializer {
  @Override
  public byte[] serialize(Object object) {
    return org.ray.runtime.util.Serializer.encode(object);
  }

  @Override
  public <T> T deserialize(byte[] bytes) {
    return org.ray.runtime.util.Serializer.decode(bytes);
  }
}
