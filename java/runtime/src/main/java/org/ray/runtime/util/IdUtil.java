package org.ray.runtime.util;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.ray.api.id.BaseId;
import org.ray.api.id.ObjectId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;


/**
 * Helper method for different Ids.
 * Note: any changes to these methods must be synced with C++ helper functions
 * in src/ray/id.h
 */
public class IdUtil {
  public static final int OBJECT_INDEX_POS = 16;

  /**
   * Compute the object ID of an object returned by the task.
   *
   * @param taskId The task ID of the task that created the object.
   * @param returnIndex What number return value this object is in the task.
   * @return The computed object ID.
   */
  public static ObjectId computeReturnId(TaskId taskId, int returnIndex) {
    return computeObjectId(taskId, returnIndex);
  }

  /**
   * Compute the object ID from the task ID and the index.
   * @param taskId The task ID of the task that created the object.
   * @param index The index which can distinguish different objects in one task.
   * @return The computed object ID.
   */
  private static ObjectId computeObjectId(TaskId taskId, int index) {
    byte[] bytes = new byte[ObjectId.LENGTH];
    System.arraycopy(taskId.getBytes(), 0, bytes, 0, taskId.size());
    ByteBuffer wbb = ByteBuffer.wrap(bytes);
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    wbb.putInt(OBJECT_INDEX_POS, index);
    return new ObjectId(bytes);
  }

  /**
   * Compute the object ID of an object put by the task.
   *
   * @param taskId The task ID of the task that created the object.
   * @param putIndex What number put this object was created by in the task.
   * @return The computed object ID.
   */
  public static ObjectId computePutId(TaskId taskId, int putIndex) {
    // We multiply putIndex by -1 to distinguish from returnIndex.
    return computeObjectId(taskId, -1 * putIndex);
  }

  /**
   * Generate the return ids of a task.
   *
   * @param taskId The ID of the task that generates returnsIds.
   * @param numReturns The number of returnIds.
   * @return The Return Ids of this task.
   */
  public static ObjectId[] genReturnIds(TaskId taskId, int numReturns) {
    ObjectId[] ret = new ObjectId[numReturns];
    for (int i = 0; i < numReturns; i++) {
      ret[i] = IdUtil.computeReturnId(taskId, i + 1);
    }
    return ret;
  }

  public static <T extends BaseId> byte[][] getIdBytes(List<T> objectIds) {
    int size = objectIds.size();
    byte[][] ids = new byte[size][];
    for (int i = 0; i < size; i++) {
      ids[i] = objectIds.get(i).getBytes();
    }
    return ids;
  }

  public static byte[][] getByteListFromByteBuffer(ByteBuffer byteBufferOfIds, int length) {
    Preconditions.checkArgument(byteBufferOfIds != null);

    byte[] bytesOfIds = new byte[byteBufferOfIds.remaining()];
    byteBufferOfIds.get(bytesOfIds, 0, byteBufferOfIds.remaining());

    int count = bytesOfIds.length / length;
    byte[][] idBytes = new byte[count][];

    for (int i = 0; i < count; ++i) {
      byte[] id = new byte[length];
      System.arraycopy(bytesOfIds, i * length, id, 0, length);
      idBytes[i] = id;
    }

    return idBytes;
  }

  /**
   * Get unique IDs from concatenated ByteBuffer.
   *
   * @param byteBufferOfIds The ByteBuffer concatenated from IDs.
   * @return The array of unique IDs.
   */
  public static UniqueId[] getUniqueIdsFromByteBuffer(ByteBuffer byteBufferOfIds) {
    byte[][]idBytes = getByteListFromByteBuffer(byteBufferOfIds, UniqueId.LENGTH);
    UniqueId[] uniqueIds = new UniqueId[idBytes.length];

    for (int i = 0; i < idBytes.length; ++i) {
      uniqueIds[i] = UniqueId.fromByteBuffer(ByteBuffer.wrap(idBytes[i]));
    }

    return uniqueIds;
  }

  /**
   * Get object IDs from concatenated ByteBuffer.
   *
   * @param byteBufferOfIds The ByteBuffer concatenated from IDs.
   * @return The array of object IDs.
   */
  public static ObjectId[] getObjectIdsFromByteBuffer(ByteBuffer byteBufferOfIds) {
    byte[][]idBytes = getByteListFromByteBuffer(byteBufferOfIds, UniqueId.LENGTH);
    ObjectId[] objectIds = new ObjectId[idBytes.length];

    for (int i = 0; i < idBytes.length; ++i) {
      objectIds[i] = ObjectId.fromByteBuffer(ByteBuffer.wrap(idBytes[i]));
    }

    return objectIds;
  }

  /**
   * Concatenate IDs to a ByteBuffer.
   *
   * @param ids The array of IDs that will be concatenated.
   * @return A ByteBuffer that contains bytes of concatenated IDs.
   */
  public static <T extends BaseId> ByteBuffer concatIds(T[] ids) {
    int length = 0;
    if (ids != null && ids.length != 0) {
      length = ids[0].size() * ids.length;
    }
    byte[] bytesOfIds = new byte[length];
    for (int i = 0; i < ids.length; ++i) {
      System.arraycopy(ids[i].getBytes(), 0, bytesOfIds,
          i * ids[i].size(), ids[i].size());
    }

    return ByteBuffer.wrap(bytesOfIds);
  }


  /**
   * Compute the murmur hash code of this ID.
   */
  public static long murmurHashCode(BaseId id) {
    return murmurHash64A(id.getBytes(), id.size(), 0);
  }

  /**
   * This method is the same as `hash()` method of `ID` class in ray/src/ray/id.h
   */
  private static long murmurHash64A(byte[] data, int length, int seed) {
    final long m = 0xc6a4a7935bd1e995L;
    final int r = 47;

    long h = (seed & 0xFFFFFFFFL) ^ (length * m);

    int length8 = length / 8;

    for (int i = 0; i < length8; i++) {
      final int i8 = i * 8;
      long k =  ((long)data[i8] & 0xff)
          + (((long)data[i8 + 1] & 0xff) << 8)
          + (((long)data[i8 + 2] & 0xff) << 16)
          + (((long)data[i8 + 3] & 0xff) << 24)
          + (((long)data[i8 + 4] & 0xff) << 32)
          + (((long)data[i8 + 5] & 0xff) << 40)
          + (((long)data[i8 + 6] & 0xff) << 48)
          + (((long)data[i8 + 7] & 0xff) << 56);

      k *= m;
      k ^= k >>> r;
      k *= m;

      h ^= k;
      h *= m;
    }

    final int remaining = length % 8;
    if (remaining >= 7) {
      h ^= (long) (data[(length & ~7) + 6] & 0xff) << 48;
    }
    if (remaining >= 6) {
      h ^= (long) (data[(length & ~7) + 5] & 0xff) << 40;
    }
    if (remaining >= 5) {
      h ^= (long) (data[(length & ~7) + 4] & 0xff) << 32;
    }
    if (remaining >= 4) {
      h ^= (long) (data[(length & ~7) + 3] & 0xff) << 24;
    }
    if (remaining >= 3) {
      h ^= (long) (data[(length & ~7) + 2] & 0xff) << 16;
    }
    if (remaining >= 2) {
      h ^= (long) (data[(length & ~7) + 1] & 0xff) << 8;
    }
    if (remaining >= 1) {
      h ^= (long) (data[length & ~7] & 0xff);
      h *= m;
    }

    h ^= h >>> r;
    h *= m;
    h ^= h >>> r;

    return h;
  }
}
