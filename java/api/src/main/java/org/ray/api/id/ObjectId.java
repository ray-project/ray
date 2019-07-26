package org.ray.api.id;

import org.ray.api.ObjectType;
import org.ray.api.TransportType;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

/**
 * Represents the id of a Ray object.
 */
public class ObjectId extends BaseId implements Serializable {

  public static final int LENGTH = 20;

  public static final ObjectId NIL = genNil();

  private static int IS_TASK_FLAG_BITS_OFFSET = 15;

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
  public static ObjectId forPut(TaskId taskId, int put_index) {
    return forPut(taskId, put_index, TransportType.STANDARD);
  }

  /**
   * Compute the object ID of an object put by the task.
   */
  public static ObjectId forPut(TaskId taskId, int put_index, TransportType transportType) {
    short flags = 0;
    flags = setIsTaskFlag(flags, true);
    flags = setObjectTypeFlag(flags, ObjectType.PUT_OBJECT);
    flags = setTransportTypeFlags(flags, transportType);

    byte[] bytes = new byte[ObjectId.LENGTH];
    System.arraycopy(taskId.getBytes(), 0, bytes, 0, TaskId.LENGTH);

    ByteBuffer wbb = ByteBuffer.wrap(bytes);
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    wbb.putShort(FLAGS_BYTES_POS, flags);

    wbb.putInt(INDEX_BYTES_POS, put_index);
    return new ObjectId(bytes);
  }

  /**
   * Compute the object ID of an object return by the task.
   */
  public static ObjectId forReturn(TaskId taskId, int return_index) {
    return forReturn(taskId, return_index, TransportType.STANDARD);
  }

  /**
   * Compute the object ID of an object return by the task.
   */
  public static ObjectId forReturn(TaskId taskId, int return_index, TransportType transportType) {
    short flags = 0;
    flags = setIsTaskFlag(flags, true);
    flags = setObjectTypeFlag(flags, ObjectType.RETURN_OBJECT);
    flags = setTransportTypeFlags(flags, transportType);

    byte[] bytes = new byte[ObjectId.LENGTH];
    System.arraycopy(taskId.getBytes(), 0, bytes, 0, TaskId.LENGTH);

    ByteBuffer wbb = ByteBuffer.wrap(bytes);
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    wbb.putShort(FLAGS_BYTES_POS, flags);

    wbb.putInt(INDEX_BYTES_POS, return_index);
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
    return TaskId.fromByteBuffer(ByteBuffer.wrap(taskIdBytes));
  }

  private static short setIsTaskFlag(short flags, boolean is_task) {
    if (is_task) {
      return (short) (flags | (0x1 << IS_TASK_FLAG_BITS_OFFSET));
    } else {
      return (short) (flags | (0x0 << IS_TASK_FLAG_BITS_OFFSET));
    }
  }

  private static short setObjectTypeFlag(short flags, ObjectType objectType) {
    if (objectType == ObjectType.RETURN_OBJECT) {
      return (short)(flags | (0x1 << OBJECT_TYPE_FLAG_BITS_OFFSET));
    } else {
      return (short)(flags | (0x0 << OBJECT_TYPE_FLAG_BITS_OFFSET));
    }
  }

  private static short setTransportTypeFlags(short flags, TransportType transportType) {
    if (transportType == TransportType.DIRECT_ACTOR_CALL) {
      return (short)(0x1 << TRANSPORT_TYPE_FLAG_BITS_OFFSET);
    } else {
      return (short)(flags | (0x0 << OBJECT_TYPE_FLAG_BITS_OFFSET));
    }
  }

}
