package org.ray.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.ray.api.UniqueID;


//
// Helper methods for UniqueID. These are the same as the helper functions in src/ray/id.h.
//
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
  public static UniqueID computeReturnId(UniqueID taskId, int returnIndex) {
    return computeObjectId(taskId, returnIndex);
  }

  /**
   * Compute the object ID from the task ID and the index.
   * @param taskId The task ID of the task that created the object.
   * @param index The index which can distinguish different objects in one task.
   * @return The computed object ID.
   */
  private static UniqueID computeObjectId(UniqueID taskId, int index) {
    byte[] objId = new byte[UniqueID.LENGTH];
    System.arraycopy(taskId.getBytes(),0, objId, 0, UniqueID.LENGTH);
    ByteBuffer wbb = ByteBuffer.wrap(objId);
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    wbb.putInt(UniqueIdHelper.OBJECT_INDEX_POS, index);

    return new UniqueID(objId);
  }

  /**
   * Compute the object ID of an object put by the task.
   *
   * @param taskId The task ID of the task that created the object.
   * @param putIndex What number put this object was created by in the task.
   * @return The computed object ID.
   */
  public static UniqueID computePutId(UniqueID taskId, int putIndex) {
    // We multiply putIndex by -1 to distinguish from returnIndex.
    return computeObjectId(taskId, -1 * putIndex);
  }

  /**
   * Compute the task ID of the task that created the object.
   *
   * @param objectId The object ID.
   * @return The task ID of the task that created this object.
   */
  public static UniqueID computeTaskId(UniqueID objectId) {
    byte[] taskId = new byte[UniqueID.LENGTH];
    System.arraycopy(objectId.getBytes(), 0, taskId, 0, UniqueID.LENGTH);
    Arrays.fill(taskId, UniqueIdHelper.OBJECT_INDEX_POS,
        UniqueIdHelper.OBJECT_INDEX_POS + UniqueIdHelper.OBJECT_INDEX_LENGTH, (byte) 0);

    return new UniqueID(taskId);
  }

}
