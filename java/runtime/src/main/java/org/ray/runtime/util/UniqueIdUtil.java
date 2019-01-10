package org.ray.runtime.util;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.ray.api.id.UniqueId;


/**
 * Helper method for UniqueId.
 * Note: any changes to these methods must be synced with C++ helper functions
 * in src/ray/id.h
 */
public class UniqueIdUtil {
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
    wbb.putInt(UniqueIdUtil.OBJECT_INDEX_POS, index);

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
    Arrays.fill(taskId, UniqueIdUtil.OBJECT_INDEX_POS,
        UniqueIdUtil.OBJECT_INDEX_POS + UniqueIdUtil.OBJECT_INDEX_LENGTH, (byte) 0);

    return new UniqueId(taskId);
  }

  public static byte[][] getIdBytes(List<UniqueId> objectIds) {
    int size = objectIds.size();
    byte[][] ids = new byte[size][];
    for (int i = 0; i < size; i++) {
      ids[i] = objectIds.get(i).getBytes();
    }
    return ids;
  }

  /**
   * Get unique IDs from concatenated ByteBuffer.
   *
   * @param byteBufferOfIds The ByteBuffer concatenated from IDs.
   * @return The array of unique IDs.
   */
  public static UniqueId[] getUniqueIdsFromByteBuffer(ByteBuffer byteBufferOfIds) {
    Preconditions.checkArgument(byteBufferOfIds != null);

    byte[] bytesOfIds = new byte[byteBufferOfIds.remaining()];
    byteBufferOfIds.get(bytesOfIds, 0, byteBufferOfIds.remaining());

    int count = bytesOfIds.length / UniqueId.LENGTH;
    UniqueId[] uniqueIds = new UniqueId[count];

    for (int i = 0; i < count; ++i) {
      byte[] id = new byte[UniqueId.LENGTH];
      System.arraycopy(bytesOfIds, i * UniqueId.LENGTH, id, 0, UniqueId.LENGTH);
      uniqueIds[i] = UniqueId.fromByteBuffer(ByteBuffer.wrap(id));
    }

    return uniqueIds;
  }

  /**
   * Concatenate IDs to a ByteBuffer.
   *
   * @param ids The array of IDs that will be concatenated.
   * @return A ByteBuffer that contains bytes of concatenated IDs.
   */
  public static ByteBuffer concatUniqueIds(UniqueId[] ids) {
    byte[] bytesOfIds = new byte[UniqueId.LENGTH * ids.length];
    for (int i = 0; i < ids.length; ++i) {
      System.arraycopy(ids[i].getBytes(), 0, bytesOfIds,
          i * UniqueId.LENGTH, UniqueId.LENGTH);
    }

    return ByteBuffer.wrap(bytesOfIds);
  }
}
