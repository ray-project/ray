package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class ActorGroupId extends BaseId implements Serializable {

  private static final int UNIQUE_BYTES_LENGTH = 4;

  public static final int LENGTH = JobId.LENGTH + UNIQUE_BYTES_LENGTH;

  public static final ActorGroupId NIL = nil();

  private ActorGroupId(byte[] id) {
    super(id);
  }

  public static ActorGroupId fromByteBuffer(ByteBuffer bb) {
    return new ActorGroupId(byteBuffer2Bytes(bb));
  }

  public static ActorGroupId fromBytes(byte[] bytes) {
    return new ActorGroupId(bytes);
  }

  /**
   * Generate a nil ActorGroupId.
   */
  private static ActorGroupId nil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new ActorGroupId(b);
  }

  /**
   * Generate an ActorGroupId with random value. Used for local mode and test only.
   */
  public static ActorGroupId fromRandom() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    return new ActorGroupId(b);
  }

  @Override
  public int size() {
    return LENGTH;
  }
}
