package org.ray.streaming.runtime.core.common;

import java.io.Serializable;
import java.util.Random;

/**
 * Uniquely identifies the abstract base class.
 */
public class AbstractID implements Comparable<AbstractID>, Serializable {

  private static final long serialVersionUID = 1L;
  private static final Random RND = new Random();
  private static final int SIZE_OF_LONG = 8;

  public static final int SIZE = 2 * SIZE_OF_LONG;

  protected final long upperPart;
  protected final long lowerPart;

  private String toString;

  public AbstractID(byte[] bytes) {
    if (bytes == null || bytes.length != SIZE) {
      throw new IllegalArgumentException("Argument bytes must by an array of " + SIZE + " bytes");
    }

    this.lowerPart = byteArrayToLong(bytes, 0);
    this.upperPart = byteArrayToLong(bytes, SIZE_OF_LONG);
  }

  public AbstractID(long lowerPart, long upperPart) {
    this.lowerPart = lowerPart;
    this.upperPart = upperPart;
  }

  public AbstractID(AbstractID id) {
    if (id == null) {
      throw new IllegalArgumentException("Id must not be null.");
    }
    this.lowerPart = id.lowerPart;
    this.upperPart = id.upperPart;
  }

  public AbstractID() {
    this.lowerPart = RND.nextLong();
    this.upperPart = RND.nextLong();
  }

  public long getLowerPart() {
    return lowerPart;
  }

  public long getUpperPart() {
    return upperPart;
  }

  public byte[] getBytes() {
    byte[] bytes = new byte[SIZE];
    longToByteArray(lowerPart, bytes, 0);
    longToByteArray(upperPart, bytes, SIZE_OF_LONG);
    return bytes;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj != null && obj.getClass() == getClass()) {
      AbstractID that = (AbstractID) obj;
      return that.lowerPart == this.lowerPart && that.upperPart == this.upperPart;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return ((int)  this.lowerPart) ^
        ((int) (this.lowerPart >>> 32)) ^
        ((int)  this.upperPart) ^
        ((int) (this.upperPart >>> 32));
  }

  @Override
  public String toString() {
    if (this.toString == null) {
      final byte[] ba = new byte[SIZE];
      longToByteArray(this.lowerPart, ba, 0);
      longToByteArray(this.upperPart, ba, SIZE_OF_LONG);
      this.toString = new String(ba);
    }

    return this.toString;
  }

  @Override
  public int compareTo(AbstractID o) {
    int diff1 = Long.compare(this.upperPart, o.upperPart);
    int diff2 = Long.compare(this.lowerPart, o.lowerPart);
    return diff1 == 0 ? diff2 : diff1;
  }

  private static long byteArrayToLong(byte[] ba, int offset) {
    long l = 0;

    for (int i = 0; i < SIZE_OF_LONG; ++i) {
      l |= (ba[offset + SIZE_OF_LONG - 1 - i] & 0xffL) << (i << 3);
    }

    return l;
  }

  private static void longToByteArray(long l, byte[] ba, int offset) {
    for (int i = 0; i < SIZE_OF_LONG; ++i) {
      final int shift = i << 3; // i * 8
      ba[offset + SIZE_OF_LONG - 1 - i] = (byte) ((l & (0xffL << shift)) >>> shift);
    }
  }
}
