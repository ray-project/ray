package io.ray.runtime.util;

import io.ray.api.id.BaseId;

/**
 * Helper method for different Ids. Note: any changes to these methods must be synced with C++
 * helper functions in src/ray/common/id.h
 */
public class IdUtil {

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
      long k = ((long) data[i8] & 0xff)
          + (((long) data[i8 + 1] & 0xff) << 8)
          + (((long) data[i8 + 2] & 0xff) << 16)
          + (((long) data[i8 + 3] & 0xff) << 24)
          + (((long) data[i8 + 4] & 0xff) << 32)
          + (((long) data[i8 + 5] & 0xff) << 40)
          + (((long) data[i8 + 6] & 0xff) << 48)
          + (((long) data[i8 + 7] & 0xff) << 56);

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
