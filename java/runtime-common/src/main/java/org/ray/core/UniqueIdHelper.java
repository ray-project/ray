package org.ray.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.ray.api.id.UniqueId;


/**
 * Helper method for UniqueId.
 * Note: any changes to these methods must be synced with C++ helper functions
 * in src/ray/id.h
 */
public class UniqueIdHelper {
  public static final int OBJECT_INDEX_POS = 0;
  public static final int OBJECT_INDEX_LENGTH = 4;

  /**
   * Compute the object ID of an object returned by the task.
   *
   * @param taskId The task ID of the task that created the object.
   * @param returnIndex What number return value this object is in the task.
   * @return The computed object ID.
   */
  public static UniqueId computeReturnId(UniqueId taskId, int returnIndex) {
    return computeObjectId(taskId, returnIndex);
  }

  /**
   * Compute the object ID from the task ID and the index.
   * @param taskId The task ID of the task that created the object.
   * @param index The index which can distinguish different objects in one task.
   * @return The computed object ID.
   */
  private static UniqueId computeObjectId(UniqueId taskId, int index) {
    byte[] objId = new byte[UniqueId.LENGTH];
    System.arraycopy(taskId.getBytes(),0, objId, 0, UniqueId.LENGTH);
    ByteBuffer wbb = ByteBuffer.wrap(objId);
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    wbb.putInt(UniqueIdHelper.OBJECT_INDEX_POS, index);

    return new UniqueId(objId);
  }

  /**
   * Compute the object ID of an object put by the task.
   *
   * @param taskId The task ID of the task that created the object.
   * @param putIndex What number put this object was created by in the task.
   * @return The computed object ID.
   */
  public static UniqueId computePutId(UniqueId taskId, int putIndex) {
    // We multiply putIndex by -1 to distinguish from returnIndex.
    return computeObjectId(taskId, -1 * putIndex);
  }

  /**
   * Compute the task ID of the task that created the object.
   *
   * @param objectId The object ID.
   * @return The task ID of the task that created this object.
   */
  public static UniqueId computeTaskId(UniqueId objectId) {
    byte[] taskId = new byte[UniqueId.LENGTH];
    System.arraycopy(objectId.getBytes(), 0, taskId, 0, UniqueId.LENGTH);
    Arrays.fill(taskId, UniqueIdHelper.OBJECT_INDEX_POS,
        UniqueIdHelper.OBJECT_INDEX_POS + UniqueIdHelper.OBJECT_INDEX_LENGTH, (byte) 0);

    return new UniqueId(taskId);
  }

}
