package io.ray.runtime.object.newserialization;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import io.ray.runtime.object.newserialization.serializers.ByteArraySerializer;

public class NewObjectSerializer {

  public static final Map<String, RaySerializer> CLS_TO_SERIALIZER_MAP = new HashMap<>();
  public static final Map<ByteBuffer, String> TYPE_ID_TO_CLS_MAP = new HashMap<>();

  static {
    registerSerializer(byte[].class, ByteArraySerializer.TYPE_ID, new ByteArraySerializer());
  }

  public static void registerSerializer(Class<?> cls, ByteBuffer typeId, RaySerializer serializer) {
    CLS_TO_SERIALIZER_MAP.put(cls.getCanonicalName(), serializer);
    TYPE_ID_TO_CLS_MAP.put(typeId, cls.getCanonicalName());
  }

  public static Object deserialize(RaySerializationResult result) {
    String className = TYPE_ID_TO_CLS_MAP.get(result.typeId);
    return CLS_TO_SERIALIZER_MAP.get(className).deserialize(result.inBandBuffer, result.outOfBandBuffers);
  }

  public static boolean hasSerializer(Class<?> cls) {
    return CLS_TO_SERIALIZER_MAP.containsKey(cls.getCanonicalName());
  }

  public static RaySerializationResult serialize(Object object) {
    return CLS_TO_SERIALIZER_MAP.get(object.getClass().getCanonicalName()).serialize(object);
  }

}
