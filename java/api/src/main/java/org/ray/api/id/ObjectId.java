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

  public static final ObjectId NIL = genNil();

  /**
   * Create an ObjectId from a ByteBuffer.
   */
  public static ObjectId fromByteBuffer(ByteBuffer bb) {
    return new ObjectId(byteBuffer2Bytes(bb));
  }

  /**
   * Generate a nil ObjectId.
   */
  private static ObjectId genNil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new ObjectId(b);
  }

  /**
   * Generate an ObjectId with random value.
   */
  public static ObjectId fromRandom() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
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
