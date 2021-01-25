package io.ray.streaming.runtime.util;

import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

/** Based on org.apache.spark.unsafe.Platform */
public final class Platform {

  public static final Unsafe UNSAFE;

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
  }

  // Access fields and constructors once and store them, for performance:
  private static final Constructor<?> DBB_CONSTRUCTOR;
  private static final long BUFFER_ADDRESS_FIELD_OFFSET;
  private static final long BUFFER_CAPACITY_FIELD_OFFSET;

  static {
    try {
      Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
      Constructor<?> constructor = cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
      constructor.setAccessible(true);
      DBB_CONSTRUCTOR = constructor;
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

  private static final ThreadLocal<ByteBuffer> localEmptyBuffer =
      ThreadLocal.withInitial(
          () -> {
            try {
              return (ByteBuffer) DBB_CONSTRUCTOR.newInstance(0, 0);
            } catch (InstantiationException
                | IllegalAccessException
                | InvocationTargetException e) {
              UNSAFE.throwException(e);
            }
            throw new IllegalStateException("unreachable");
          });

  /** Wrap a buffer [address, address + size) as a DirectByteBuffer. */
  public static ByteBuffer wrapDirectBuffer(long address, int size) {
    ByteBuffer buffer = localEmptyBuffer.get().duplicate();
    UNSAFE.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
    UNSAFE.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
    buffer.clear();
    return buffer;
  }

  /** Wrap a buffer [address, address + size) into provided <code>buffer</code>. */
  public static void wrapDirectBuffer(ByteBuffer buffer, long address, int size) {
    UNSAFE.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
    UNSAFE.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
    buffer.clear();
  }

  /**
   * @param buffer a DirectBuffer backed by off-heap memory
   * @return address of off-heap memory
   */
  public static long getAddress(ByteBuffer buffer) {
    return ((DirectBuffer) buffer).address();
  }
}
