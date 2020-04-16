package io.ray.streaming.runtime.serialization;

public interface Serializer {

  byte[] serialize(Object object);

  <T> T deserialize(byte[] bytes);

}
