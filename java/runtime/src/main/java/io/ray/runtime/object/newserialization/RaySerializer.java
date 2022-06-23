package io.ray.runtime.object.newserialization;

import java.nio.ByteBuffer;
import java.util.Map;

public interface RaySerializer<T> {

  RaySerializationResult serialize(T obj);

  T deserialize(
      ByteBuffer inBandBuffer, Map<ByteBuffer, Map<ByteBuffer, ByteBuffer>> outOfBandBuffers);
}
