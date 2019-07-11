package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

/**
 * Represents the id of a Ray task.
 */
public class TaskId extends BaseId implements Serializable {

  private static final int UNIQUE_BYTES_LENGTH = 6;

  public static final int LENGTH = UNIQUE_BYTES_LENGTH + ActorId.LENGTH;

  public static final TaskId NIL = genNil();

  /**
   * Create a TaskId from a hex string.
   */
  public static TaskId fromHexString(String hex) {
    return new TaskId(hexString2Bytes(hex));
  }

  /**
   * Creates a TaskId from a ByteBuffer.
   */
  public static TaskId fromByteBuffer(ByteBuffer bb) {
    return new TaskId(byteBuffer2Bytes(bb));
  }

  public static TaskId fromRandom(ActorId actorId) {
    byte[] uniqueBytes = new byte[TaskId.UNIQUE_BYTES_LENGTH];
    new Random().nextBytes(uniqueBytes);

    byte[] bytes = new byte[TaskId.LENGTH];
    ByteBuffer wbb = ByteBuffer.wrap(bytes);
    wbb.order(ByteOrder.LITTLE_ENDIAN);

    System.arraycopy(uniqueBytes, 0, bytes, 0, TaskId.UNIQUE_BYTES_LENGTH);
    System.arraycopy(actorId.getBytes(), 0, bytes, TaskId.UNIQUE_BYTES_LENGTH, ActorId.LENGTH);
    return new TaskId(bytes);
  }

  public ActorId getActorId() {
    byte[] actorIdbytes = new byte[ActorId.LENGTH];
    System.arraycopy(getBytes(), UNIQUE_BYTES_LENGTH, actorIdbytes, 0, ActorId.LENGTH);
    return ActorId.fromByteBuffer(ByteBuffer.wrap(actorIdbytes));
  }

  /**
   * Generate a nil TaskId.
   */
  private static TaskId genNil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new TaskId(b);
  }

  private TaskId(byte[] id) {
    super(id);
  }

  @Override
  public int size() {
    return LENGTH;
  }
}
