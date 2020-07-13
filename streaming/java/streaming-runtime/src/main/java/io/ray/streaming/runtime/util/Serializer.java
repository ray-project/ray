package io.ray.streaming.runtime.util;

public class Serializer {
  public static byte[] encode(Object obj) {
    return KryoUtils.writeToByteArray(obj);
  }

  public static <T> T decode(byte[] bs) {
    return KryoUtils.readFromByteArray(bs);
  }

}
