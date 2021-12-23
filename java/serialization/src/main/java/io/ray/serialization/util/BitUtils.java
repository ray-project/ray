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
}
