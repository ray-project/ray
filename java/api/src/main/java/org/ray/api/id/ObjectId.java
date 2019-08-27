package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

/**
 * Represents the id of a Ray object.
 */
public class ObjectId extends BaseId implements Serializable {

  public static final int LENGTH = 20;

  private static final int FLAGS_LENGTH = 2;

  /**
   * Create an ObjectId from a ByteBuffer.
   */
  public static ObjectId fromByteBuffer(ByteBuffer bb) {
    return new ObjectId(byteBuffer2Bytes(bb));
  }

  /**
   * Generate an ObjectId with random value.
   */
  public static ObjectId fromRandom() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    // Fill all bits of flags to zero to set the following flag values:
    // Is task: false
    // Object type: put
    // Transport type: raylet
    Arrays.fill(b, TaskId.LENGTH, TaskId.LENGTH + FLAGS_LENGTH, (byte) 0);
    return new ObjectId(b);
  }

  public ObjectId(byte[] id) {
    super(id);
  }

  @Override
  public int size() {
    return LENGTH;
  }

}
