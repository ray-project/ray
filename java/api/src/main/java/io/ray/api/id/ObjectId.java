package io.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

/**
 * Represents the id of a Ray object.
 */
public class ObjectId extends BaseId implements Serializable {

  public static final int LENGTH = 20;

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
    // This is tightly coupled with ObjectID definition in C++. If that changes,
    // this must be changed as well.
    // The following logic should be kept consistent with `ObjectID::FromRandom` in
    // C++.
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    Arrays.fill(b, TaskId.LENGTH, LENGTH, (byte) 0);
    return new ObjectId(b);
  }

  public ObjectId(byte[] id) {
    super(id);
  }

  @Override
  public int size() {
    return LENGTH;
  }

  /**
   * Compute the task ID of the task that created the object.
   * @return The task ID of the task that created this object.
   */
  public TaskId taskId() {
    byte[] taskId = new byte[TaskId.LENGTH];
    System.arraycopy(getBytes(), 0, taskId, 0, TaskId.LENGTH);
    return TaskId.fromBytes(taskId);
  }
}
