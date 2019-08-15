package org.ray.runtime.util;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.ray.api.id.BaseId;
import org.ray.api.id.ObjectId;
import org.ray.api.id.ActorId;

/**
 * Helper method for different Ids.
 * Note: any changes to these methods must be synced with C++ helper functions
 * in src/ray/common/id.h
 */
public class IdUtil {

  public static <T extends BaseId> byte[][] getIdBytes(List<T> objectIds) {
    int size = objectIds.size();
    byte[][] ids = new byte[size][];
    for (int i = 0; i < size; i++) {
      ids[i] = objectIds.get(i).getBytes();
    }
    return ids;
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
   * A helper function to compute actor creation dummy object id according
   * the given actor id.
   */
  public static ObjectId computeActorCreationDummyObjectId(ActorId actorId) {
    byte[] bytes = new byte[ObjectId.LENGTH];
    System.arraycopy(actorId.getBytes(), 0, bytes, 0, ActorId.LENGTH);
    Arrays.fill(bytes, ActorId.LENGTH, bytes.length, (byte) 0xFF);
    return ObjectId.fromByteBuffer(ByteBuffer.wrap(bytes));
  }

}
