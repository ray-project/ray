package io.ray.runtime.object.newserialization;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

public class RaySerializationResult {

  public ByteBuffer typeId;
  public ByteBuffer inBandBuffer;
  // type ID -> buffer ID -> buffer
  public Map<ByteBuffer, Map<ByteBuffer, ByteBuffer>> outOfBandBuffers;

  public byte[] toBytes() throws IOException {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    packer
        .packBinaryHeader(typeId.capacity())
        .addPayload(typeId.array())
        .packBinaryHeader(inBandBuffer.capacity())
        .addPayload(inBandBuffer.array());
    if (outOfBandBuffers != null) {
      packer.packMapHeader(outOfBandBuffers.size());
      for (Map.Entry<ByteBuffer, Map<ByteBuffer, ByteBuffer>> entry : outOfBandBuffers.entrySet()) {
        packer.packBinaryHeader(entry.getKey().capacity()).addPayload(entry.getKey().array());
        packer.packMapHeader(entry.getValue().size());
        for (Map.Entry<ByteBuffer, ByteBuffer> entry2 : entry.getValue().entrySet()) {
          packer.packBinaryHeader(entry2.getKey().capacity()).addPayload(entry2.getKey().array());
          packer
              .packBinaryHeader(entry2.getValue().capacity())
              .addPayload(entry2.getValue().array());
        }
      }
    }
    return packer.toByteArray();
  }

  public static RaySerializationResult fromBytes(byte[] data) throws IOException {
    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data);
    RaySerializationResult res = new RaySerializationResult();
    int tuple_size = unpacker.unpackArrayHeader(); // should be 3
    Preconditions.checkArgument(
        tuple_size == 3, String.format("Error size should be 3! %d got!", tuple_size));
    // byteId
    int bytes_size = unpacker.unpackBinaryHeader();
    res.typeId = ByteBuffer.allocate(bytes_size);
    unpacker.readPayload(res.typeId);
    // inBandBuffer
    bytes_size = unpacker.unpackBinaryHeader();
    res.inBandBuffer = ByteBuffer.allocate(bytes_size);
    unpacker.readPayload(res.inBandBuffer);
    // outOfBandBuffers
    int map1_size = unpacker.unpackMapHeader();
    res.outOfBandBuffers = new HashMap<>(map1_size);
    for (int i = 0; i < map1_size; ++i) {
      // key1
      int key1_size = unpacker.unpackBinaryHeader();
      ByteBuffer key1 = ByteBuffer.allocate(key1_size);
      unpacker.readPayload(key1);

      // value, still a map
      int map2_size = unpacker.unpackMapHeader();
      Map<ByteBuffer, ByteBuffer> map2 = new HashMap<>(map2_size);
      res.outOfBandBuffers.put(key1, map2);
      for (int j = 0; j < map2_size; ++j) {
        // key2
        int key2_size = unpacker.unpackBinaryHeader();
        ByteBuffer key2 = ByteBuffer.allocate(key2_size);
        unpacker.readPayload(key2);
        // value
        int value_size = unpacker.unpackBinaryHeader();
        ByteBuffer value = ByteBuffer.allocate(value_size);
        unpacker.readPayload(value);

        map2.put(key2, value);
      }
    }
    return res;
  }
}
