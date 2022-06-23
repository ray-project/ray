package io.ray.runtime.object.newserialization.serializers;

import io.ray.runtime.object.newserialization.RaySerializationResult;
import io.ray.runtime.object.newserialization.RaySerializer;
import java.nio.ByteBuffer;
import java.util.Map;

public class ByteArraySerializer implements RaySerializer<byte[]> {

  public static final ByteBuffer TYPE_ID = ByteBuffer.wrap("ray_serde_bytes".getBytes());

  @Override
  public RaySerializationResult serialize(byte[] obj) {
    RaySerializationResult res = new RaySerializationResult();
    res.inBandBuffer = ByteBuffer.wrap(obj);
    return res;
  }

  @Override
  public byte[] deserialize(
      ByteBuffer inBandBuffer, Map<ByteBuffer, Map<ByteBuffer, ByteBuffer>> outOfBandBuffers) {
    return inBandBuffer.array();
  }
}
