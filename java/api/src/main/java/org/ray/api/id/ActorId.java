package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

public class ActorId extends BaseId implements Serializable {
  private static final int UNIQUE_BYTES_LENGTH = 4;

  public static final int LENGTH = UNIQUE_BYTES_LENGTH + JobId.LENGTH;

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

  public static ActorId generateActorId(JobId jobId) {
    byte[] uniqueBytes = new byte[ActorId.UNIQUE_BYTES_LENGTH];
    new Random().nextBytes(uniqueBytes);

    byte[] bytes = new byte[ActorId.LENGTH];
    ByteBuffer wbb = ByteBuffer.wrap(bytes);
    wbb.order(ByteOrder.LITTLE_ENDIAN);

    System.arraycopy(uniqueBytes, 0, bytes, 0, ActorId.UNIQUE_BYTES_LENGTH);
    System.arraycopy(jobId.getBytes(), 0, bytes, ActorId.UNIQUE_BYTES_LENGTH, JobId.LENGTH);
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

  @Override
  public int size() {
    return LENGTH;
  }
}
