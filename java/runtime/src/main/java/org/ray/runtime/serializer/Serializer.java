package org.ray.runtime.serializer;

public class Serializer {
  public static class Meta {
    public boolean isCrossLanguage = true;
  }

  public static void setClassloader(ClassLoader classLoader) {
    MessagePackSerializer.setClassloader(classLoader);
  }

  public static byte[] encode(Object obj, Meta meta) {
    return MessagePackSerializer.encode(obj, meta,null);
  }

  public static byte[] encode(Object obj, Meta meta, ClassLoader classLoader) {
    return MessagePackSerializer.encode(obj, meta, classLoader);
  }

  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs, Class<?> type) {
    return MessagePackSerializer.decode(bs, type, null);
  }

  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs, Class<?> type, ClassLoader classLoader) {
    return MessagePackSerializer.decode(bs, type, classLoader);
  }
}
