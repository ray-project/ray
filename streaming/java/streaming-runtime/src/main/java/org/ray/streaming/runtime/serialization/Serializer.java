package org.ray.streaming.runtime.serialization;

public interface Serializer {

  byte[] serialize(Object object);

  Object deserialize(byte[] bytes);

}
