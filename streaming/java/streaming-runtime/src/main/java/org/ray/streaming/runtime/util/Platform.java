package org.ray.streaming.runtime.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

/**
 * Based on org.apache.spark.unsafe.Platform
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

  static {
    boolean _unaligned;
    String arch = System.getProperty("os.arch", "");
    if (arch.equals("ppc64le") || arch.equals("ppc64")) {
      // Since java.nio.Bits.unaligned() doesn't return true on ppc (See JDK-8165231), but
      // ppc64 and ppc64le support it
      _unaligned = true;
    } else {
      try {
        Class<?> bitsClass =
            Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
        Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
        unalignedMethod.setAccessible(true);
        _unaligned = Boolean.TRUE.equals(unalignedMethod.invoke(null));
      } catch (Throwable t) {
        // We at least know x86 and x64 support unaligned access.
        //noinspection DynamicRegexReplaceableByCompiledPattern
        _unaligned = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64|aarch64)$");
      }
    }
    unaligned = _unaligned;
  }

  /**
   * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to
   * allow safepoint polling during a large copy.
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

  private static final Constructor<?> DBB_CONSTRUCTOR;
  private static final Field DBB_CLEANER_FIELD;
  private static final long BUFFER_ADDRESS_FIELD_OFFSET;
  private static final long BUFFER_CAPACITY_FIELD_OFFSET;

  static {
    try {
      Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
      Constructor<?> constructor = cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
      constructor.setAccessible(true);
      Field cleanerField = cls.getDeclaredField("cleaner");
      cleanerField.setAccessible(true);
      DBB_CONSTRUCTOR = constructor;
      DBB_CLEANER_FIELD = cleanerField;
      Field addressField = Buffer.class.getDeclaredField("address");
      BUFFER_ADDRESS_FIELD_OFFSET = UNSAFE.objectFieldOffset(addressField);
      Preconditions.checkArgument(BUFFER_ADDRESS_FIELD_OFFSET != 0);
      Field capacityField = Buffer.class.getDeclaredField("capacity");
      BUFFER_CAPACITY_FIELD_OFFSET = UNSAFE.objectFieldOffset(capacityField);
      Preconditions.checkArgument(BUFFER_CAPACITY_FIELD_OFFSET != 0);
    } catch (ClassNotFoundException | NoSuchMethodException | NoSuchFieldException e) {
      throw new IllegalStateException(e);
    }
  }

  private static final Method CLEANER_CREATE_METHOD;

  static {
    // The implementation of Cleaner changed from JDK 8 to 9
    // Split java.version on non-digit chars:
    int majorVersion = Integer.parseInt(System.getProperty("java.version").split("\\D+")[0]);
    String cleanerClassName;
    if (majorVersion < 9) {
      cleanerClassName = "sun.misc.Cleaner";
    } else {
      cleanerClassName = "jdk.internal.ref.Cleaner";
    }
    try {
      Class<?> cleanerClass = Class.forName(cleanerClassName);
      Method createMethod = cleanerClass.getMethod("create", Object.class, Runnable.class);
      // Accessing jdk.internal.ref.Cleaner should actually fail by default in JDK 9+,
      // unfortunately, unless the user has allowed access with something like
      // --add-opens java.base/java.lang=ALL-UNNAMED  If not, we can't really use the Cleaner
      // hack below. It doesn't break, just means the user might run into the default JVM limit
      // on off-heap memory and increase it or set the flag above. This tests whether it's
      // available:
      try {
        createMethod.invoke(null, null, null);
      } catch (IllegalAccessException e) {
        // Don't throw an exception, but can't log here?
        createMethod = null;
      } catch (InvocationTargetException ite) {
        // shouldn't happen; report it
        throw new IllegalStateException(ite);
      }
      CLEANER_CREATE_METHOD = createMethod;
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      throw new IllegalStateException(e);
    }

  }

  /**
   * @return true when running JVM is having sun's Unsafe package available in it and underlying
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

  /**
   * Allocate a DirectByteBuffer, potentially bypassing the JVM's MaxDirectMemorySize limit.
   */
  public static ByteBuffer allocateDirectBuffer(int size) {
    try {
      if (CLEANER_CREATE_METHOD == null) {
        // Can't set a Cleaner (see comments on field), so need to allocate via normal Java APIs
        try {
          return ByteBuffer.allocateDirect(size);
        } catch (OutOfMemoryError oome) {
          // checkstyle.off: RegexpSinglelineJava
          throw new OutOfMemoryError("Failed to allocate direct buffer (" + oome.getMessage() +
              "); try increasing -XX:MaxDirectMemorySize=... to, for example, your heap size");
          // checkstyle.on: RegexpSinglelineJava
        }
      }
      // Otherwise, use internal JDK APIs to allocate a DirectByteBuffer while ignoring the JVM's
      // MaxDirectMemorySize limit (the default limit is too low and we do not want to
      // require users to increase it).
      long memory = allocateMemory(size);
      ByteBuffer buffer = (ByteBuffer) DBB_CONSTRUCTOR.newInstance(memory, size);
      try {
        DBB_CLEANER_FIELD.set(buffer,
            CLEANER_CREATE_METHOD.invoke(null, buffer, (Runnable) () -> freeMemory(memory)));
      } catch (IllegalAccessException | InvocationTargetException e) {
        freeMemory(memory);
        throw new IllegalStateException(e);
      }
      return buffer;
    } catch (Exception e) {
      throwException(e);
    }
    throw new IllegalStateException("unreachable");
  }

  private static final ThreadLocal<ByteBuffer> localEmptyBuffer =
      ThreadLocal.withInitial(() -> {
        try {
          return (ByteBuffer) DBB_CONSTRUCTOR.newInstance(0, 0);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
          throwException(e);
        }
        throw new IllegalStateException("unreachable");
      });

  public static ByteBuffer wrapDirectBuffer(long address, int size) {
    ByteBuffer buffer = localEmptyBuffer.get().duplicate();
    UNSAFE.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
    UNSAFE.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
    buffer.clear();
    return buffer;
  }

  /**
   * Wrap a buffer [address, address + size) into provided <code>buffer</code>.
   */
  public static void wrapDirectBuffer(ByteBuffer buffer, long address, int size) {
    UNSAFE.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
    UNSAFE.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
    buffer.clear();
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
        if (Platform.getByte(leftBase, leftOffset + i) !=
            Platform.getByte(rightBase, rightOffset + i)) {
          return false;
        }
        i += 1;
      }
    }
    // for architectures that support unaligned accesses, chew it up 8 bytes at a time
    if (unaligned || (((leftOffset + i) % 8 == 0) && ((rightOffset + i) % 8 == 0))) {
      while (i <= length - 8) {
        if (Platform.getLong(leftBase, leftOffset + i) !=
            Platform.getLong(rightBase, rightOffset + i)) {
          return false;
        }
        i += 8;
      }
    }
    // this will finish off the unaligned comparisons, or do the entire aligned
    // comparison whichever is needed.
    while (i < length) {
      if (Platform.getByte(leftBase, leftOffset + i) !=
          Platform.getByte(rightBase, rightOffset + i)) {
        return false;
      }
      i += 1;
    }
    return true;
  }

  /**
   * Raises an exception bypassing compiler checks for checked exceptions.
   */
  public static void throwException(Throwable t) {
    UNSAFE.throwException(t);
  }

  /**
   * Create an instance of <code>type</code>. This method don't call constructor.
   */
  public static <T> T newInstance(Class<T> type) {
    try {
      return type.cast(UNSAFE.allocateInstance(type));
    } catch (InstantiationException e) {
      throwException(e);
    }
    throw new IllegalStateException("unreachable");
  }

  // --------------------------------------------------------------------------------------------
  //  Utilities for native memory accesses and checks
  // --------------------------------------------------------------------------------------------
  public static long getAddress(ByteBuffer buffer) {
    return ((DirectBuffer) buffer).address();
  }

  public static long checkBufferAndGetAddress(ByteBuffer buffer) {
    if (buffer == null) {
      throw new NullPointerException("buffer is null");
    }
    if (!buffer.isDirect()) {
      throw new NullPointerException("buffer isn't direct");
    }
    return ((DirectBuffer) buffer).address();
  }
}
