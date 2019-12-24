package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class ActorId extends BaseId implements Serializable {

  private static final int UNIQUE_BYTES_LENGTH = 4;

  public static final int LENGTH = JobId.LENGTH + UNIQUE_BYTES_LENGTH;

  public static final ActorId NIL = nil();

  private ActorId(byte[] id) {
    super(id);
  }

  public static ActorId fromByteBuffer(ByteBuffer bb) {
    return new ActorId(byteBuffer2Bytes(bb));
  }

  public static ActorId fromBytes(byte[] bytes) {
    return new ActorId(bytes);
  }

  /**
   * Generate a nil ActorId.
   */
  private static ActorId nil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new ActorId(b);
  }

  /**
   * Generate an ActorId with random value. Used for local mode and test only.
   */
  public static ActorId fromRandom() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    return new ActorId(b);
  }

  @Override
  public int size() {
    return LENGTH;
  }
}
