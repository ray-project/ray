package io.ray.streaming.runtime.util;

import io.ray.runtime.serializer.FstSerializer;

public class Serializer {

  public static byte[] encode(Object obj) {
    return FstSerializer.encode(obj);
  }

  public static <T> T decode(byte[] bytes) {
    return FstSerializer.decode(bytes);
  }
}
