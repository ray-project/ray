package org.ray.runtime.util;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.ray.api.id.BaseId;
import org.ray.api.id.ObjectId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.api.id.ActorId;

/**
 * Helper method for different Ids.
 * Note: any changes to these methods must be synced with C++ helper functions
 * in src/ray/common/id.h
 */
public class IdUtil {
  public static final int OBJECT_INDEX_POS = 14;

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
   * Compute the murmur hash code of this ID.
   */
  public static long murmurHashCode(BaseId id) {
    return murmurHash64A(id.getBytes(), id.size(), 0);
  }

  /**
   * This method is the same as `Hash()` method of `ID` class in ray/src/ray/common/id.h
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

  /*
   A temp helper to generate a ObjectId according to the given actorId.
   */
  public static ObjectId computeActorCreationDummyObjectId(ActorId actorId) {
    byte[] bytes = new byte[ObjectId.LENGTH];
    System.arraycopy(actorId.getBytes(), 0, bytes, 0, ActorId.LENGTH);
    Arrays.fill(bytes, ActorId.LENGTH, bytes.length, (byte) 0xFF);
    return ObjectId.fromByteBuffer(ByteBuffer.wrap(bytes));
  }

}
