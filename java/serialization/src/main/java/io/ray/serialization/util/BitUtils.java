package io.ray.serialization.util;

/** We assume that the bitmap data is word-aligned (that is, a multiple of 8 bytes in length) */
public class BitUtils {

  private static final int WORD_SIZE = 8;

  /**
   * Sets the bit at the specified index to {@code true}.
   *
   * <p>Every byte is set form right to left(the least significant -> the most significant): 1L <<
   * bitIndex
   */
  public static void set(MemoryBuffer validityBuffer, int baseOffset, int index) {
    final int byteIndex = baseOffset + (index >> 3);
    final int bitIndex = index & 7;
    byte currentByte = validityBuffer.get(byteIndex);
    final byte bitMask = (byte) (1L << bitIndex);
    currentByte |= bitMask;
    validityBuffer.put(byteIndex, currentByte);
  }

  public static void setAll(MemoryBuffer validityBuffer, int baseOffset, int valueCount) {
    final int sizeInBytes = (valueCount + 7) / 8;
    // If value count is not a multiple of 8, then calculate number of used bits in the last byte
    final int remainder = valueCount % 8;

    final int sizeInBytesMinus1 = sizeInBytes - 1;
    int bytesMinus1EndOffset = baseOffset + sizeInBytesMinus1;
    for (int i = baseOffset; i < bytesMinus1EndOffset; i++) {
      validityBuffer.put(i, (byte) 0xff);
    }

    // handling with the last byte
    // since unsafe putLong use native byte order, maybe not big endian,
    // see java.nio.DirectByteBuffer.putLong(long, long), we can't use unsafe.putLong
    // for bit operation, native byte order may be subject to change between machine
    if (remainder != 0) {
      // Every byte is set form right to left
      byte byteValue = (byte) (0xff >>> (8 - remainder));
      validityBuffer.put(baseOffset + sizeInBytesMinus1, byteValue);
    }
  }

  public static void unset(MemoryBuffer validityBuffer, int baseOffset, int index) {
    setValidityBit(validityBuffer, baseOffset, index, 0);
  }

  /** Set the bit at a given index to provided value (1 or 0). */
  public static void setValidityBit(
      MemoryBuffer validityBuffer, int baseOffset, int index, int value) {
    final int byteIndex = baseOffset + (index >> 3);
    final int bitIndex = index & 7;
    byte currentByte = validityBuffer.get(byteIndex);
    final byte bitMask = (byte) (1L << bitIndex);
    if (value != 0) {
      currentByte |= bitMask;
    } else {
      currentByte -= (bitMask & currentByte);
    }
    validityBuffer.put(byteIndex, currentByte);
  }

  public static boolean isSet(MemoryBuffer validityBuffer, int baseOffset, int index) {
    final int byteIndex = baseOffset + (index >> 3);
    final int bitIndex = index & 7;
    final byte b = validityBuffer.get(byteIndex);
    return ((b >> bitIndex) & 0x01) != 0;
  }

  public static boolean isNotSet(MemoryBuffer validityBuffer, int baseOffset, int index) {
    final int byteIndex = baseOffset + (index >> 3);
    final int bitIndex = index & 7;
    final byte b = validityBuffer.get(byteIndex);
    return ((b >> bitIndex) & 0x01) == 0;
  }

  /** Returns {@code true} if any bit is set. */
  public static boolean anySet(
      MemoryBuffer validityBuffer, int baseOffset, int bitmapWidthInBytes) {
    int addr = baseOffset;
    int bitmapWidthInWords = bitmapWidthInBytes / WORD_SIZE;
    for (int i = 0; i < bitmapWidthInWords; i++, addr += WORD_SIZE) {
      if (validityBuffer.getLong(addr) != 0) {
        return true;
      }
    }
    return false;
  }

  /** Returns {@code true} if any bit is not set. */
  public static boolean anyUnSet(MemoryBuffer validityBuffer, int baseOffset, int valueCount) {
    final int sizeInBytes = (valueCount + 7) / 8;
    // If value count is not a multiple of 8, then calculate number of used bits in the last byte
    final int remainder = valueCount % 8;

    final int sizeInBytesMinus1 = sizeInBytes - 1;
    int bytesMinus1EndOffset = baseOffset + sizeInBytesMinus1;
    for (int i = baseOffset; i < bytesMinus1EndOffset; i++) {
      if (validityBuffer.get(i) != (byte) 0xFF) {
        return true;
      }
    }

    // handling with the last byte
    // since unsafe putLong use native byte order, maybe not big endian,
    // see java.nio.DirectByteBuffer.putLong(long, long), we can't use unsafe.putLong
    // for bit operation, native byte order may be subject to change between machine,
    // so we use getByte
    if (remainder != 0) {
      byte byteValue = validityBuffer.get(baseOffset + sizeInBytesMinus1);
      // Every byte is set form right to left
      byte mask = (byte) (0xFF >>> (8 - remainder));
      return byteValue != mask;
    }

    return false;
  }

  /**
   * Given a validity buffer, find the number of bits that are not set. This is used to compute the
   * number of null elements in a nullable vector.
   *
   * <p>Every byte is set form right to left: 0xFF << remainder
   *
   * @return number of bits not set.
   */
  public static int getNullCount(
      final MemoryBuffer validityBuffer, int baseOffset, int valueCount) {
    // not null count + remainder
    int count = 0;
    final int sizeInBytes = (valueCount + 7) / 8;
    // If value count is not a multiple of 8, then calculate number of used bits in the last byte
    final int remainder = valueCount % 8;

    final int sizeInBytesMinus1 = sizeInBytes - 1;
    int bytesMinus1EndOffset = baseOffset + sizeInBytesMinus1;
    for (int i = baseOffset; i < bytesMinus1EndOffset; i++) {
      byte byteValue = validityBuffer.get(i);
      // byteValue & 0xFF: sets int to the (unsigned) 8 bits value resulting from
      // putting the 8 bits of value in the lowest 8 bits of int.
      count += Integer.bitCount(byteValue & 0xFF);
    }

    // handling with the last byte
    byte byteValue = validityBuffer.get(baseOffset + sizeInBytesMinus1);
    if (remainder != 0) {
      // making the remaining bits all 1s if it is not fully filled
      byte mask = (byte) (0xFF << remainder);
      byteValue = (byte) (byteValue | mask);
    }
    count += Integer.bitCount(byteValue & 0xFF);

    return 8 * sizeInBytes - count;
  }

  public static int calculateBitmapWidthInBytes(int numFields) {
    return ((numFields + 63) / 64) * WORD_SIZE;
  }
}
