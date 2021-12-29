package io.ray.runtime.io;

import com.google.common.base.Preconditions;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import sun.misc.Unsafe;

/**
 * A utility class for unsafe memory operations. Note: This class is based on
 * org.apache.spark.unsafe.Platform
 */
public final class Platform {

  public static final Unsafe UNSAFE;

  public static final int BOOLEAN_ARRAY_OFFSET;

  public static final int BYTE_ARRAY_OFFSET;

  public static final int CHAR_ARRAY_OFFSET;

  public static final int SHORT_ARRAY_OFFSET;

  public static final int INT_ARRAY_OFFSET;

  public static final int LONG_ARRAY_OFFSET;

  public static final int FLOAT_ARRAY_OFFSET;

  public static final int DOUBLE_ARRAY_OFFSET;

  private static final boolean unaligned;

  private static final Class<?> DIRECT_BYTE_BUFFER_CLASS =
      getClassByName("java.nio.DirectByteBuffer");

  static {
    // Check whether the underlying system having unaligned-access capability.
    boolean unalign;
    String arch = System.getProperty("os.arch", "");
    if (arch.equals("ppc64le") || arch.equals("ppc64")) {
      // Since java.nio.Bits.unaligned() doesn't return true on ppc (See JDK-8165231), but
      // ppc64 and ppc64le support it
      unalign = true;
    } else {
      try {
        Class<?> bitsClass =
            Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
        Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
        unalignedMethod.setAccessible(true);
        unalign = Boolean.TRUE.equals(unalignedMethod.invoke(null));
      } catch (Throwable t) {
        // We at least know x86 and x64 support unaligned access.
        //noinspection DynamicRegexReplaceableByCompiledPattern
        unalign = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64|aarch64)$");
      }
    }
    unaligned = unalign;
  }

  /**
   * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to allow
   * safepoint polling during a large copy.
   */
  private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

  static {
    Unsafe unsafe;
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (Unsafe) unsafeField.get(null);
    } catch (Throwable cause) {
      throw new UnsupportedOperationException("Unsafe is not supported in this platform.");
    }
    UNSAFE = unsafe;
    BOOLEAN_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(boolean[].class);
    BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    CHAR_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(char[].class);
    SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
    INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
    LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
    FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
    DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);
  }

  // Access fields and constructors once and store them, for performance:

  private static final long BUFFER_ADDRESS_FIELD_OFFSET;
  private static final long BUFFER_CAPACITY_FIELD_OFFSET;

  static {
    try {
      Field addressField = Buffer.class.getDeclaredField("address");
      BUFFER_ADDRESS_FIELD_OFFSET = UNSAFE.objectFieldOffset(addressField);
      Preconditions.checkArgument(BUFFER_ADDRESS_FIELD_OFFSET != 0);
      Field capacityField = Buffer.class.getDeclaredField("capacity");
      BUFFER_CAPACITY_FIELD_OFFSET = UNSAFE.objectFieldOffset(capacityField);
      Preconditions.checkArgument(BUFFER_CAPACITY_FIELD_OFFSET != 0);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Class<?> getClassByName(@SuppressWarnings("SameParameterValue") String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns true when running JVM is having sun's Unsafe package available in it and underlying
   * system having unaligned-access capability.
   */
  public static boolean unaligned() {
    return unaligned;
  }

  public static int getInt(Object object, long offset) {
    return UNSAFE.getInt(object, offset);
  }

  public static void putInt(Object object, long offset, int value) {
    UNSAFE.putInt(object, offset, value);
  }

  public static boolean getBoolean(Object object, long offset) {
    return UNSAFE.getBoolean(object, offset);
  }

  public static void putBoolean(Object object, long offset, boolean value) {
    UNSAFE.putBoolean(object, offset, value);
  }

  public static byte getByte(Object object, long offset) {
    return UNSAFE.getByte(object, offset);
  }

  public static void putByte(Object object, long offset, byte value) {
    UNSAFE.putByte(object, offset, value);
  }

  public static short getShort(Object object, long offset) {
    return UNSAFE.getShort(object, offset);
  }

  public static void putShort(Object object, long offset, short value) {
    UNSAFE.putShort(object, offset, value);
  }

  public static long getLong(Object object, long offset) {
    return UNSAFE.getLong(object, offset);
  }

  public static void putLong(Object object, long offset, long value) {
    UNSAFE.putLong(object, offset, value);
  }

  public static float getFloat(Object object, long offset) {
    return UNSAFE.getFloat(object, offset);
  }

  public static void putFloat(Object object, long offset, float value) {
    UNSAFE.putFloat(object, offset, value);
  }

  public static double getDouble(Object object, long offset) {
    return UNSAFE.getDouble(object, offset);
  }

  public static void putDouble(Object object, long offset, double value) {
    UNSAFE.putDouble(object, offset, value);
  }

  public static Object getObjectVolatile(Object object, long offset) {
    return UNSAFE.getObjectVolatile(object, offset);
  }

  public static void putObjectVolatile(Object object, long offset, Object value) {
    UNSAFE.putObjectVolatile(object, offset, value);
  }

  public static long allocateMemory(long size) {
    return UNSAFE.allocateMemory(size);
  }

  public static void freeMemory(long address) {
    UNSAFE.freeMemory(address);
  }

  public static long reallocateMemory(long address, long oldSize, long newSize) {
    long newMemory = UNSAFE.allocateMemory(newSize);
    copyMemory(null, address, null, newMemory, oldSize);
    freeMemory(address);
    return newMemory;
  }

  public static void setMemory(Object object, long offset, long size, byte value) {
    UNSAFE.setMemory(object, offset, size, value);
  }

  public static void setMemory(long address, byte value, long size) {
    UNSAFE.setMemory(address, size, value);
  }

  public static void copyMemory(
      Object src, long srcOffset, Object dst, long dstOffset, long length) {
    // Check if dstOffset is before or after srcOffset to determine if we should copy
    // forward or backwards. This is necessary in case src and dst overlap.
    if (dstOffset < srcOffset) {
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
        srcOffset += size;
        dstOffset += size;
      }
    } else {
      srcOffset += length;
      dstOffset += length;
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        srcOffset -= size;
        dstOffset -= size;
        UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
      }
    }
  }

  /**
   * Optimized byte array equality check for byte arrays.
   *
   * @return true if the arrays are equal, false otherwise
   */
  public static boolean arrayEquals(
      Object leftBase, long leftOffset, Object rightBase, long rightOffset, final long length) {
    int i = 0;

    // check if stars align and we can get both offsets to be aligned
    if ((leftOffset % 8) == (rightOffset % 8)) {
      while ((leftOffset + i) % 8 != 0 && i < length) {
        if (Platform.getByte(leftBase, leftOffset + i)
            != Platform.getByte(rightBase, rightOffset + i)) {
          return false;
        }
        i += 1;
      }
    }
    // for architectures that support unaligned accesses, chew it up 8 bytes at a time
    if (unaligned || (((leftOffset + i) % 8 == 0) && ((rightOffset + i) % 8 == 0))) {
      while (i <= length - 8) {
        if (Platform.getLong(leftBase, leftOffset + i)
            != Platform.getLong(rightBase, rightOffset + i)) {
          return false;
        }
        i += 8;
      }
    }
    // this will finish off the unaligned comparisons, or do the entire aligned
    // comparison whichever is needed.
    while (i < length) {
      if (Platform.getByte(leftBase, leftOffset + i)
          != Platform.getByte(rightBase, rightOffset + i)) {
        return false;
      }
      i += 1;
    }
    return true;
  }

  /** Raises an exception bypassing compiler checks for checked exceptions. */
  public static void throwException(Throwable t) {
    UNSAFE.throwException(t);
  }

  /** Create an instance of <code>type</code>. This method don't call constructor. */
  public static <T> T newInstance(Class<T> type) {
    try {
      return type.cast(UNSAFE.allocateInstance(type));
    } catch (InstantiationException e) {
      throwException(e);
    }
    throw new IllegalStateException("unreachable");
  }

  public static long getAddress(ByteBuffer buffer) {
    Preconditions.checkNotNull(buffer, "buffer is null");
    Preconditions.checkArgument(buffer.isDirect(), "Can't get address of a non-direct ByteBuffer.");
    long offHeapAddress;
    try {
      offHeapAddress = UNSAFE.getLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET);
    } catch (Throwable t) {
      throw new Error("Could not access direct byte buffer address field.", t);
    }
    return offHeapAddress;
  }

  private static final ByteBuffer localBuffer = ByteBuffer.allocateDirect(0);

  /** Create a direct buffer from native memory represented by address [address, address + size). */
  public static ByteBuffer createDirectByteBufferFromNativeAddress(long address, int size) {
    try {
      // ByteBuffer.allocateDirect(0) is about 30x slower than `localBuffer.duplicate()`.
      ByteBuffer buffer = localBuffer.duplicate();
      UNSAFE.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
      UNSAFE.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
      buffer.clear();
      return buffer;
    } catch (Throwable t) {
      throw new Error("Failed to wrap unsafe off-heap memory with ByteBuffer", t);
    }
  }

  /** Wrap a buffer [address, address + size) into provided <code>buffer</code>. */
  public static void wrapDirectByteBufferFromNativeAddress(
      ByteBuffer buffer, long address, int size) {
    Preconditions.checkArgument(
        buffer.isDirect(), "Can't wrap native memory into a non-direct ByteBuffer.");
    UNSAFE.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
    UNSAFE.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
    buffer.clear();
  }
}
