package io.ray.streaming.runtime.serialization;

public interface Serializer {
  byte JAVA_TYPE_ID = 0;
  byte CROSS_LANG_TYPE_ID = 1;

  byte[] serialize(Object object);

  <T> T deserialize(byte[] bytes);

}
