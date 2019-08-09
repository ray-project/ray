package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;
import org.ray.api.ObjectType;

/**
 * Represents the id of a Ray object.
 */
public class ObjectId extends BaseId implements Serializable {

  public static final int LENGTH = 20;

  public static final ObjectId NIL = genNil();

  private static int CREATED_BY_TASK_FLAG_BITS_OFFSET = 15;

  private static int OBJECT_TYPE_FLAG_BITS_OFFSET = 14;

  private static int TRANSPORT_TYPE_FLAG_BITS_OFFSET = 11;

  private static int FLAGS_BYTES_POS = TaskId.LENGTH;

  private static int FLAGS_BYTES_LENGTH = 2;

  private static int INDEX_BYTES_POS = FLAGS_BYTES_POS + FLAGS_BYTES_LENGTH;

  private static int INDEX_BYTES_LENGTH = 4;

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

  /**
   * Compute the object ID of an object put by the task.
   */
  public static ObjectId forPut(TaskId taskId, int putIndex) {
    short flags = 0;
    flags = setCreatedByTaskFlag(flags, true);
    // Set a default transport type with value 0.
    flags = (short) (flags | (0x0 << TRANSPORT_TYPE_FLAG_BITS_OFFSET));
    flags = setObjectTypeFlag(flags, ObjectType.PUT_OBJECT);

    byte[] bytes = new byte[ObjectId.LENGTH];
    System.arraycopy(taskId.getBytes(), 0, bytes, 0, TaskId.LENGTH);

    ByteBuffer wbb = ByteBuffer.wrap(bytes);
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    wbb.putShort(FLAGS_BYTES_POS, flags);

    wbb.putInt(INDEX_BYTES_POS, putIndex);
    return new ObjectId(bytes);
  }

  /**
   * Compute the object ID of an object return by the task.
   */
  public static ObjectId forReturn(TaskId taskId, int returnIndex) {
    short flags = 0;
    flags = setCreatedByTaskFlag(flags, true);
    // Set a default transport type with value 0.
    flags = (short) (flags | (0x0 << TRANSPORT_TYPE_FLAG_BITS_OFFSET));
    flags = setObjectTypeFlag(flags, ObjectType.RETURN_OBJECT);

    byte[] bytes = new byte[ObjectId.LENGTH];
    System.arraycopy(taskId.getBytes(), 0, bytes, 0, TaskId.LENGTH);

    ByteBuffer wbb = ByteBuffer.wrap(bytes);
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    wbb.putShort(FLAGS_BYTES_POS, flags);

    wbb.putInt(INDEX_BYTES_POS, returnIndex);
    return new ObjectId(bytes);
  }

  public ObjectId(byte[] id) {
    super(id);
  }

  @Override
  public int size() {
    return LENGTH;
  }

  public TaskId getTaskId() {
    byte[] taskIdBytes = Arrays.copyOf(getBytes(), TaskId.LENGTH);
    return TaskId.fromBytes(taskIdBytes);
  }

  private static short setCreatedByTaskFlag(short flags, boolean createdByTask) {
    if (createdByTask) {
      return (short) (flags | (0x1 << CREATED_BY_TASK_FLAG_BITS_OFFSET));
    } else {
      return (short) (flags | (0x0 << CREATED_BY_TASK_FLAG_BITS_OFFSET));
    }
  }

  private static short setObjectTypeFlag(short flags, ObjectType objectType) {
    if (objectType == ObjectType.RETURN_OBJECT) {
      return (short)(flags | (0x1 << OBJECT_TYPE_FLAG_BITS_OFFSET));
    } else {
      return (short)(flags | (0x0 << OBJECT_TYPE_FLAG_BITS_OFFSET));
    }
  }

}
