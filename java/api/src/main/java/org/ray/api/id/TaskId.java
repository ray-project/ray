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

  /**
   * Creates a TaskId from a given bytes.
   */
  public static TaskId fromBytes(byte[] bytes) {
    return new TaskId(bytes);
  }

  /**
   * Creates a TaskId for actor creation task.
   */
  public static TaskId forActorCreationTask(ActorId actorId) {
    byte[] nilBytes = new byte[TaskId.UNIQUE_BYTES_LENGTH];
    Arrays.fill(nilBytes, 0, TaskId.UNIQUE_BYTES_LENGTH, (byte) 0xFF);

    byte[] bytes = new byte[TaskId.LENGTH];
    ByteBuffer wbb = ByteBuffer.wrap(bytes);
    wbb.order(ByteOrder.LITTLE_ENDIAN);

    System.arraycopy(nilBytes, 0, bytes, 0, TaskId.UNIQUE_BYTES_LENGTH);
    System.arraycopy(actorId.getBytes(), 0, bytes, TaskId.UNIQUE_BYTES_LENGTH, ActorId.LENGTH);
    return new TaskId(bytes);
  }

  // TODO(qwang): Fix
  /**
   * Creates a TaskId for actor task.
   */
  public static TaskId forActorTask(ActorId actorId) {
    byte[] uniqueBytes = new byte[TaskId.UNIQUE_BYTES_LENGTH];
    new Random().nextBytes(uniqueBytes);

    byte[] bytes = new byte[TaskId.LENGTH];
    ByteBuffer wbb = ByteBuffer.wrap(bytes);
    wbb.order(ByteOrder.LITTLE_ENDIAN);

    System.arraycopy(uniqueBytes, 0, bytes, 0, TaskId.UNIQUE_BYTES_LENGTH);
    System.arraycopy(actorId.getBytes(), 0, bytes, TaskId.UNIQUE_BYTES_LENGTH, ActorId.LENGTH);
    return new TaskId(bytes);
  }

  /**
   * Creates a TaskId for normal task.
   */
  public static TaskId forNormalTask() {
    byte[] bytes = new byte[TaskId.LENGTH];
    new Random().nextBytes(bytes);
    return new TaskId(bytes);
  }

  public ActorId getActorId() {
    byte[] actorIdBytes = new byte[ActorId.LENGTH];
    System.arraycopy(getBytes(), UNIQUE_BYTES_LENGTH, actorIdBytes, 0, ActorId.LENGTH);
    return ActorId.fromByteBuffer(ByteBuffer.wrap(actorIdBytes));
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
