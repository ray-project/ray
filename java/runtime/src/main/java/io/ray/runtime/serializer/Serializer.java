package io.ray.runtime.serializer;

import org.apache.commons.lang3.tuple.Pair;

public class Serializer {

  public static Pair<byte[], Boolean> encode(Object obj) {
    return MessagePackSerializer.encode(obj);
  }

  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs, Class<?> type) {
    return MessagePackSerializer.decode(bs, type);
  }
}
