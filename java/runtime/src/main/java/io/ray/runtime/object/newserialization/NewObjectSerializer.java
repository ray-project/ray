package io.ray.runtime.object.newserialization;

import io.ray.runtime.object.newserialization.serializers.BytesInBandSerializer;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewObjectSerializer {

  public static final Map<String, RaySerializer> CLS_TO_SERIALIZER_MAP = new HashMap<>();
  public static final Map<ByteBuffer, String> TYPE_ID_TO_CLS_MAP = new HashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(NewObjectSerializer.class);

  static {
    registerSerializer(byte[].class, BytesInBandSerializer.TYPE_ID, new BytesInBandSerializer());
  }

  public static void registerSerializer(Class<?> cls, ByteBuffer typeId, RaySerializer serializer) {
    CLS_TO_SERIALIZER_MAP.put(cls.getCanonicalName(), serializer);
    TYPE_ID_TO_CLS_MAP.put(typeId, cls.getCanonicalName());
  }

  public static Object deserialize(RaySerializationResult result) {
    String className = TYPE_ID_TO_CLS_MAP.get(result.typeId);
    LOG.debug(
        "Deserializing, typeId={}, className={}, TYPE_ID_TO_CLS_MAP={}",
        result.typeId.toString(),
        className,
        TYPE_ID_TO_CLS_MAP);
    return CLS_TO_SERIALIZER_MAP
        .get(className)
        .deserialize(result.inBandBuffer, result.outOfBandBuffers);
  }

  public static boolean hasSerializer(Class<?> cls) {
    return CLS_TO_SERIALIZER_MAP.containsKey(cls.getCanonicalName());
  }

  public static RaySerializationResult serialize(Object object) {
    return CLS_TO_SERIALIZER_MAP.get(object.getClass().getCanonicalName()).serialize(object);
  }
}
