package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class GroupId extends BaseId implements Serializable {

  private static final int UNIQUE_BYTES_LENGTH = 4;

  public static final int LENGTH = JobId.LENGTH + UNIQUE_BYTES_LENGTH;

  public static final GroupId NIL = nil();

  private GroupId(byte[] id) {
    super(id);
  }

  public static GroupId fromByteBuffer(ByteBuffer bb) {
    return new GroupId(byteBuffer2Bytes(bb));
  }

  public static GroupId fromBytes(byte[] bytes) {
    return new GroupId(bytes);
  }

  /**
   * Generate a nil GroupId.
   */
  private static GroupId nil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new GroupId(b);
  }

  /**
   * Generate an GroupId with random value. Used for local mode and test only.
   */
  public static GroupId fromRandom() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    return new GroupId(b);
  }

  @Override
  public int size() {
    return LENGTH;
  }
}
