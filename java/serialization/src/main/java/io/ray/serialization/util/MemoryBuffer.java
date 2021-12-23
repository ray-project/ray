package io.ray.serialization.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Preconditions;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;

/** Based on org.apache.flink.core.memory.MemoryBuffer */
public final class MemoryBuffer {

  /** The unsafe handle for transparent memory copied (heap / off-heap). */
  @SuppressWarnings("restriction")
  private static final sun.misc.Unsafe UNSAFE = Platform.UNSAFE;

  /** The beginning of the byte array contents, relative to the byte array object. */
  @SuppressWarnings("restriction")
  private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

  /**
   * Constant that flags the byte order. Because this is a boolean constant, the JIT compiler can
   * use this well to aggressively eliminate the non-applicable code paths.
   */
  private static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

  /**
   * The heap byte array object relative to which we access the memory.
   *
   * <p>Is non-<tt>null</tt> if the memory is on the heap, and is <tt>null</tt>, if the memory if
   * off the heap. If we have this buffer, we must never void this reference, or the memory buffer
   * will point to undefined addresses outside the heap and may in out-of-order execution cases
   * cause bufferation faults.
   */
  private byte[] heapMemory;

  /**
   * The direct byte buffer that allocated the off-heap memory. This memory buffer holds a reference
   * to that buffer, so as long as this memory buffer lives, the memory will not be released.
   */
  private ByteBuffer offHeapBuffer;

  /**
   * The address to the data, relative to the heap memory byte array. If the heap memory byte array
   * is <tt>null</tt>, this becomes an absolute memory address outside the heap.
   */
  private long address;

  /**
   * The address one byte after the last addressable byte, i.e. <tt>address + size</tt> while the
   * buffer is not disposed.
   */
  private long addressLimit;

  /** The size in bytes of the memory buffer. */
  private int size;

  private int readerIndex;
  private int writerIndex;

  /**
   * Creates a new memory buffer that represents the memory of the byte array.
   *
   * <p>Since the byte array is backed by on-heap memory, this memory buffer holds its data on heap.
   * The buffer must be at least of size 8 bytes.
   *
   * @param buffer The byte array whose memory is represented by this memory buffer.
   * @param offset The offset of the sub array to be used; must be non-negative and no larger than
   *     <tt>array.length</tt>.
   * @param length buffer size
   */
  MemoryBuffer(byte[] buffer, int offset, int length) {
    Preconditions.checkArgument(offset >= 0 && length >= 0);
    if (offset + length > buffer.length) {
      throw new IllegalArgumentException(
          String.format("%d exceeds buffer size %d", offset + length, buffer.length));
    }
    initHeapBuffer(buffer, offset, length);
  }

  /**
   * Creates a new memory buffer that represents the memory of the byte array.
   *
   * <p>The memory buffer references the given owner.
   *
   * @param buffer The byte array whose memory is represented by this memory buffer.
   */
  MemoryBuffer(byte[] buffer) {
    this(buffer, 0, buffer.length);
  }

  private void initHeapBuffer(byte[] buffer, int offset, int length) {
    if (buffer == null) {
      throw new NullPointerException("buffer");
    }
    this.heapMemory = buffer;
    this.address = BYTE_ARRAY_BASE_OFFSET + offset;
    this.size = length;
    this.addressLimit = this.address + this.size;
  }

  /**
   * Creates a new memory buffer that represents the memory backing the given direct byte buffer
   * section of [buffer.position(), buffer,limit()). Note that the given ByteBuffer must be direct
   * {@link ByteBuffer#allocateDirect(int)}, otherwise this method with throw an
   * IllegalArgumentException.
   *
   * @param buffer The byte buffer whose memory is represented by this memory buffer.
   * @throws IllegalArgumentException Thrown, if the given ByteBuffer is not direct.
   */
  MemoryBuffer(ByteBuffer buffer) {
    this(Platform.checkBufferAndGetAddress(buffer) + buffer.position(), buffer.remaining());
    this.offHeapBuffer = buffer;
  }

  /**
   * Creates a new memory buffer that represents the memory at the absolute address given by the
   * pointer.
   *
   * @param offHeapAddress The address of the memory represented by this memory buffer.
   * @param size The size of this memory buffer.
   */
  MemoryBuffer(long offHeapAddress, int size) {
    if (offHeapAddress <= 0) {
      throw new IllegalArgumentException("negative pointer or size");
    }
    if (offHeapAddress >= Long.MAX_VALUE - Integer.MAX_VALUE) {
      // this is necessary to make sure the collapsed checks are safe against numeric overflows
      throw new IllegalArgumentException(
          "Buffer initialized with too large address: "
              + offHeapAddress
              + " ; Max allowed address is "
              + (Long.MAX_VALUE - Integer.MAX_VALUE - 1));
    }

    this.heapMemory = null;
    this.address = offHeapAddress;
    this.addressLimit = this.address + size;
    this.size = size;
  }

  // ------------------------------------------------------------------------
  // Memory buffer Operations
  // ------------------------------------------------------------------------

  /**
   * Gets the size of the memory buffer, in bytes.
   *
   * @return The size of the memory buffer.
   */
  public int size() {
    return size;
  }

  /**
   * Checks whether the memory buffer was freed.
   *
   * @return <tt>true</tt>, if the memory buffer has been freed, <tt>false</tt> otherwise.
   */
  public boolean isFreed() {
    return address > addressLimit;
  }

  /**
   * Frees this memory buffer.
   *
   * <p>After this operation has been called, no further operations are possible on the memory
   * buffer and will fail. The actual memory (heap or off-heap) will only be released after this
   * memory buffer object has become garbage collected.
   */
  public void free() {
    // this ensures we can place no more data and trigger
    // the checks for the freed buffer
    address = addressLimit + 1;
  }

  /**
   * Checks whether this memory buffer is backed by off-heap memory.
   *
   * @return <tt>true</tt>, if the memory buffer is backed by off-heap memory, <tt>false</tt> if it
   *     is backed by heap memory.
   */
  public boolean isOffHeap() {
    return heapMemory == null;
  }

  /**
   * Get the heap byte array object.
   *
   * @return Return non-null if the memory is on the heap, and return null, if the memory if off the
   *     heap.
   */
  public byte[] getHeapMemory() {
    return heapMemory;
  }

  /**
   * Gets the buffer that owns the memory of this memory buffer.
   *
   * @return The byte buffer that owns the memory of this memory buffer.
   */
  public ByteBuffer getOffHeapBuffer() {
    if (offHeapBuffer != null) {
      return offHeapBuffer;
    } else {
      throw new IllegalStateException("Memory buffer does not represent off heap ByteBuffer");
    }
  }

  /**
   * Returns the byte array of on-heap memory buffers.
   *
   * @return underlying byte array
   * @throws IllegalStateException if the memory buffer does not represent on-heap memory
   */
  public byte[] getArray() {
    if (heapMemory != null) {
      return heapMemory;
    } else {
      throw new IllegalStateException("Memory buffer does not represent heap memory");
    }
  }

  /**
   * Returns the memory address of off-heap memory buffers.
   *
   * @return absolute memory address outside the heap
   * @throws IllegalStateException if the memory buffer does not represent off-heap memory
   */
  public long getAddress() {
    if (heapMemory == null) {
      return address;
    } else {
      throw new IllegalStateException("Memory buffer does not represent off heap memory");
    }
  }

  /**
   * Wraps the chunk of the underlying memory located between <tt>offset</tt> and <tt>length</tt> in
   * a NIO ByteBuffer.
   *
   * @param offset The offset in the memory buffer.
   * @param length The number of bytes to be wrapped as a buffer.
   * @return A <tt>ByteBuffer</tt> backed by the specified portion of the memory buffer.
   * @throws IndexOutOfBoundsException Thrown, if offset is negative or larger than the memory
   *     buffer size, or if the offset plus the length is larger than the buffer size.
   */
  public ByteBuffer wrap(int offset, int length) {
    if (address <= addressLimit) {
      if (heapMemory != null) {
        return ByteBuffer.wrap(heapMemory, offset, length);
      } else {
        try {
          ByteBuffer wrapper = offHeapBuffer.duplicate();
          wrapper.limit(offset + length);
          wrapper.position(offset);
          return wrapper;
        } catch (IllegalArgumentException e) {
          throw new IndexOutOfBoundsException();
        }
      }
    } else {
      throw new IllegalStateException("Buffer has been freed");
    }
  }

  // ------------------------------------------------------------------------
  //                    Random Access get() and put() methods
  // ------------------------------------------------------------------------

  // ------------------------------------------------------------------------
  // Notes on the implementation: We try to collapse as many checks as
  // possible. We need to obey the following rules to make this safe
  // against segfaults:
  //
  //  - Grab mutable fields onto the stack before checking and using. This
  //    guards us against concurrent modifications which invalidate the
  //    pointers
  //  - Use subtractions for range checks, as they are tolerant
  // ------------------------------------------------------------------------

  /**
   * Reads the byte at the given position.
   *
   * @param index The position from which the byte will be read
   * @return The byte at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger or equal to the
   *     size of the memory buffer.
   */
  public final byte get(int index) {
    final long pos = address + index;
    if (index >= 0 && pos < addressLimit) {
      return UNSAFE.getByte(heapMemory, pos);
    } else if (address > addressLimit) {
      throw new IllegalStateException("Buffer has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes the given byte into this buffer at the given position.
   *
   * @param index The index at which the byte will be written.
   * @param b The byte value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger or equal to the
   *     size of the memory buffer.
   */
  public final void put(int index, byte b) {
    final long pos = address + index;
    if (index >= 0 && pos < addressLimit) {
      UNSAFE.putByte(heapMemory, pos, b);
    } else if (address > addressLimit) {
      throw new IllegalStateException("Buffer has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Bulk get method. Copies dst.length memory from the specified position to the destination
   * memory.
   *
   * @param index The position at which the first byte will be read.
   * @param dst The memory into which the memory will be copied.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large that the data
   *     between the index and the memory buffer end is not enough to fill the destination array.
   */
  public final void get(int index, byte[] dst) {
    get(index, dst, 0, dst.length);
  }

  /**
   * Bulk put method. Copies src.length memory from the source memory into the memory buffer
   * beginning at the specified position.
   *
   * @param index The index in the memory buffer array, where the data is put.
   * @param src The source array to copy the data from.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large such that the
   *     array size exceed the amount of memory between the index and the memory buffer's end.
   */
  public final void put(int index, byte[] src) {
    put(index, src, 0, src.length);
  }

  /**
   * Bulk get method. Copies length memory from the specified position to the destination memory,
   * beginning at the given offset.
   *
   * @param index The position at which the first byte will be read.
   * @param dst The memory into which the memory will be copied.
   * @param offset The copying offset in the destination memory.
   * @param length The number of bytes to be copied.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large that the
   *     requested number of bytes exceed the amount of memory between the index and the memory
   *     buffer's end.
   */
  public final void get(int index, byte[] dst, int offset, int length) {
    // check the byte array offset and length and the status
    if ((offset | length | (offset + length) | (dst.length - (offset + length))) < 0) {
      throw new IndexOutOfBoundsException();
    }

    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - length) {
      final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
      UNSAFE.copyMemory(heapMemory, pos, dst, arrayAddress, length);
    } else if (address > addressLimit) {
      throw new IllegalStateException("Buffer has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Bulk put method. Copies length memory starting at position offset from the source memory into
   * the memory buffer starting at the specified index.
   *
   * @param index The position in the memory buffer array, where the data is put.
   * @param src The source array to copy the data from.
   * @param offset The offset in the source array where the copying is started.
   * @param length The number of bytes to copy.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large such that the
   *     array portion to copy exceed the amount of memory between the index and the memory buffer's
   *     end.
   */
  public final void put(int index, byte[] src, int offset, int length) {
    // check the byte array offset and length
    if ((offset | length | (offset + length) | (src.length - (offset + length))) < 0) {
      throw new IndexOutOfBoundsException();
    }

    final long pos = address + index;

    if (index >= 0 && pos <= addressLimit - length) {
      final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
      UNSAFE.copyMemory(src, arrayAddress, heapMemory, pos, length);
    } else if (address > addressLimit) {
      throw new IllegalStateException("Buffer has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads one byte at the given position and returns its boolean representation.
   *
   * @param index The position from which the memory will be read.
   * @return The boolean value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 1.
   */
  public final boolean getBoolean(int index) {
    return get(index) != 0;
  }

  /**
   * Writes one byte containing the byte value into this buffer at the given position.
   *
   * @param index The position at which the memory will be written.
   * @param value The char value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 1.
   */
  public final void putBoolean(int index, boolean value) {
    put(index, (byte) (value ? 1 : 0));
  }

  /**
   * Reads a char value from the given position, in the system's native byte order.
   *
   * @param index The position from which the memory will be read.
   * @return The char value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 2.
   */
  @SuppressWarnings("restriction")
  public final char getChar(int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      return UNSAFE.getChar(heapMemory, pos);
    } else if (address > addressLimit) {
      throw new IllegalStateException("This buffer has been freed.");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads a character value (16 bit, 2 bytes) from the given position, in little-endian byte order.
   * This method's speed depends on the system's native byte order, and it is possibly slower than
   * {@link #getChar(int)}. For most cases (such as transient storage in memory or serialization for
   * I/O and network), it suffices to know that the byte order in which the value is written is the
   * same as the one in which it is read, and {@link #getChar(int)} is the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The character value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 2.
   */
  public final char getCharL(int index) {
    if (LITTLE_ENDIAN) {
      return getChar(index);
    } else {
      return Character.reverseBytes(getChar(index));
    }
  }

  /**
   * Writes a char value to the given position, in the system's native byte order.
   *
   * @param index The position at which the memory will be written.
   * @param value The char value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 2.
   */
  @SuppressWarnings("restriction")
  public final void putChar(int index, char value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      UNSAFE.putChar(heapMemory, pos, value);
    } else if (address > addressLimit) {
      throw new IllegalStateException("Buffer has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes the given character (16 bit, 2 bytes) to the given position in little-endian byte order.
   * This method's speed depends on the system's native byte order, and it is possibly slower than
   * {@link #putChar(int, char)}. For most cases (such as transient storage in memory or
   * serialization for I/O and network), it suffices to know that the byte order in which the value
   * is written is the same as the one in which it is read, and {@link #putChar(int, char)} is the
   * preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The char value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 2.
   */
  public final void putCharL(int index, char value) {
    if (LITTLE_ENDIAN) {
      putChar(index, value);
    } else {
      putChar(index, Character.reverseBytes(value));
    }
  }

  /**
   * Reads a short integer value (16 bit, 2 bytes) from the given position, composing them into a
   * short value according to the current byte order.
   *
   * @param index The position from which the memory will be read.
   * @return The short value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 2.
   */
  public final short getShort(int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      return UNSAFE.getShort(heapMemory, pos);
    } else if (address > addressLimit) {
      throw new IllegalStateException("Buffer has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads a short integer value (16 bit, 2 bytes) from the given position, in little-endian byte
   * order. This method's speed depends on the system's native byte order, and it is possibly slower
   * than {@link #getShort(int)}. For most cases (such as transient storage in memory or
   * serialization for I/O and network), it suffices to know that the byte order in which the value
   * is written is the same as the one in which it is read, and {@link #getShort(int)} is the
   * preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The short value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 2.
   */
  public final short getShortL(int index) {
    if (LITTLE_ENDIAN) {
      return getShort(index);
    } else {
      return Short.reverseBytes(getShort(index));
    }
  }

  /**
   * Writes the given short value into this buffer at the given position, using the native byte
   * order of the system.
   *
   * @param index The position at which the value will be written.
   * @param value The short value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 2.
   */
  public final void putShort(int index, short value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      UNSAFE.putShort(heapMemory, pos, value);
    } else if (address > addressLimit) {
      throw new IllegalStateException("Buffer has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes the given short integer value (16 bit, 2 bytes) to the given position in little-endian
   * byte order. This method's speed depends on the system's native byte order, and it is possibly
   * slower than {@link #putShort(int, short)}. For most cases (such as transient storage in memory
   * or serialization for I/O and network), it suffices to know that the byte order in which the
   * value is written is the same as the one in which it is read, and {@link #putShort(int, short)}
   * is the preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The short value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 2.
   */
  public final void putShortL(int index, short value) {
    if (LITTLE_ENDIAN) {
      putShort(index, value);
    } else {
      putShort(index, Short.reverseBytes(value));
    }
  }

  /**
   * Reads an int value (32bit, 4 bytes) from the given position, in the system's native byte order.
   * This method offers the best speed for integer reading and should be used unless a specific byte
   * order is required. In most cases, it suffices to know that the byte order in which the value is
   * written is the same as the one in which it is read (such as transient storage in memory, or
   * serialization for I/O and network), making this method the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The int value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 4.
   */
  public final int getInt(int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 4) {
      return UNSAFE.getInt(heapMemory, pos);
    } else if (address > addressLimit) {
      throw new IllegalStateException("Buffer has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads an int value (32bit, 4 bytes) from the given position, in little-endian byte order. This
   * method's speed depends on the system's native byte order, and it is possibly slower than {@link
   * #getInt(int)}. For most cases (such as transient storage in memory or serialization for I/O and
   * network), it suffices to know that the byte order in which the value is written is the same as
   * the one in which it is read, and {@link #getInt(int)} is the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The int value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 4.
   */
  public final int getIntL(int index) {
    if (LITTLE_ENDIAN) {
      return getInt(index);
    } else {
      return Integer.reverseBytes(getInt(index));
    }
  }

  /**
   * Writes the given int value (32bit, 4 bytes) to the given position in the system's native byte
   * order. This method offers the best speed for integer writing and should be used unless a
   * specific byte order is required. In most cases, it suffices to know that the byte order in
   * which the value is written is the same as the one in which it is read (such as transient
   * storage in memory, or serialization for I/O and network), making this method the preferable
   * choice.
   *
   * @param index The position at which the value will be written.
   * @param value The int value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 4.
   */
  public final void putInt(int index, int value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 4) {
      UNSAFE.putInt(heapMemory, pos, value);
    } else if (address > addressLimit) {
      throw new IllegalStateException("Buffer has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes the given int value (32bit, 4 bytes) to the given position in little endian byte order.
   * This method's speed depends on the system's native byte order, and it is possibly slower than
   * {@link #putInt(int, int)}. For most cases (such as transient storage in memory or serialization
   * for I/O and network), it suffices to know that the byte order in which the value is written is
   * the same as the one in which it is read, and {@link #putInt(int, int)} is the preferable
   * choice.
   *
   * @param index The position at which the value will be written.
   * @param value The int value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 4.
   */
  public final void putIntL(int index, int value) {
    if (LITTLE_ENDIAN) {
      putInt(index, value);
    } else {
      putInt(index, Integer.reverseBytes(value));
    }
  }

  /**
   * Reads a long value (64bit, 8 bytes) from the given position, in the system's native byte order.
   * This method offers the best speed for long integer reading and should be used unless a specific
   * byte order is required. In most cases, it suffices to know that the byte order in which the
   * value is written is the same as the one in which it is read (such as transient storage in
   * memory, or serialization for I/O and network), making this method the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 8.
   */
  public final long getLong(int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 8) {
      return UNSAFE.getLong(heapMemory, pos);
    } else if (address > addressLimit) {
      throw new IllegalStateException("Buffer has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads a long integer value (64bit, 8 bytes) from the given position, in little endian byte
   * order. This method's speed depends on the system's native byte order, and it is possibly slower
   * than {@link #getLong(int)}. For most cases (such as transient storage in memory or
   * serialization for I/O and network), it suffices to know that the byte order in which the value
   * is written is the same as the one in which it is read, and {@link #getLong(int)} is the
   * preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 8.
   */
  public final long getLongL(int index) {
    if (LITTLE_ENDIAN) {
      return getLong(index);
    } else {
      return Long.reverseBytes(getLong(index));
    }
  }

  public final long getLongB(int index) {
    if (LITTLE_ENDIAN) {
      return Long.reverseBytes(getLong(index));
    } else {
      return getLong(index);
    }
  }

  /**
   * Writes the given long value (64bit, 8 bytes) to the given position in the system's native byte
   * order. This method offers the best speed for long integer writing and should be used unless a
   * specific byte order is required. In most cases, it suffices to know that the byte order in
   * which the value is written is the same as the one in which it is read (such as transient
   * storage in memory, or serialization for I/O and network), making this method the preferable
   * choice.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 8.
   */
  public final void putLong(int index, long value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 8) {
      UNSAFE.putLong(heapMemory, pos, value);
    } else if (address > addressLimit) {
      throw new IllegalStateException("Buffer has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes the given long value (64bit, 8 bytes) to the given position in little endian byte order.
   * This method's speed depends on the system's native byte order, and it is possibly slower than
   * {@link #putLong(int, long)}. For most cases (such as transient storage in memory or
   * serialization for I/O and network), it suffices to know that the byte order in which the value
   * is written is the same as the one in which it is read, and {@link #putLong(int, long)} is the
   * preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 8.
   */
  public final void putLongL(int index, long value) {
    if (LITTLE_ENDIAN) {
      putLong(index, value);
    } else {
      putLong(index, Long.reverseBytes(value));
    }
  }

  public final void putLongB(int index, long value) {
    if (LITTLE_ENDIAN) {
      putLong(index, Long.reverseBytes(value));
    } else {
      putLong(index, value);
    }
  }

  /**
   * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in the
   * system's native byte order. This method offers the best speed for float reading and should be
   * used unless a specific byte order is required. In most cases, it suffices to know that the byte
   * order in which the value is written is the same as the one in which it is read (such as
   * transient storage in memory, or serialization for I/O and network), making this method the
   * preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The float value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 4.
   */
  public final float getFloat(int index) {
    return Float.intBitsToFloat(getInt(index));
  }

  /**
   * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in
   * little endian byte order. This method's speed depends on the system's native byte order, and it
   * is possibly slower than {@link #getFloat(int)}. For most cases (such as transient storage in
   * memory or serialization for I/O and network), it suffices to know that the byte order in which
   * the value is written is the same as the one in which it is read, and {@link #getFloat(int)} is
   * the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 8.
   */
  public final float getFloatL(int index) {
    return Float.intBitsToFloat(getIntL(index));
  }

  /**
   * Writes the given single-precision float value (32bit, 4 bytes) to the given position in the
   * system's native byte order. This method offers the best speed for float writing and should be
   * used unless a specific byte order is required. In most cases, it suffices to know that the byte
   * order in which the value is written is the same as the one in which it is read (such as
   * transient storage in memory, or serialization for I/O and network), making this method the
   * preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The float value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 4.
   */
  public final void putFloat(int index, float value) {
    putInt(index, Float.floatToRawIntBits(value));
  }

  /**
   * Writes the given single-precision float value (32bit, 4 bytes) to the given position in little
   * endian byte order. This method's speed depends on the system's native byte order, and it is
   * possibly slower than {@link #putFloat(int, float)}. For most cases (such as transient storage
   * in memory or serialization for I/O and network), it suffices to know that the byte order in
   * which the value is written is the same as the one in which it is read, and {@link
   * #putFloat(int, float)} is the preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 8.
   */
  public final void putFloatL(int index, float value) {
    putIntL(index, Float.floatToRawIntBits(value));
  }

  /**
   * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in the
   * system's native byte order. This method offers the best speed for double reading and should be
   * used unless a specific byte order is required. In most cases, it suffices to know that the byte
   * order in which the value is written is the same as the one in which it is read (such as
   * transient storage in memory, or serialization for I/O and network), making this method the
   * preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The double value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 8.
   */
  public final double getDouble(int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  /**
   * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in
   * little endian byte order. This method's speed depends on the system's native byte order, and it
   * is possibly slower than {@link #getDouble(int)}. For most cases (such as transient storage in
   * memory or serialization for I/O and network), it suffices to know that the byte order in which
   * the value is written is the same as the one in which it is read, and {@link #getDouble(int)} is
   * the preferable choice.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 8.
   */
  public final double getDoubleL(int index) {
    return Double.longBitsToDouble(getLongL(index));
  }

  /**
   * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position
   * in the system's native byte order. This method offers the best speed for double writing and
   * should be used unless a specific byte order is required. In most cases, it suffices to know
   * that the byte order in which the value is written is the same as the one in which it is read
   * (such as transient storage in memory, or serialization for I/O and network), making this method
   * the preferable choice.
   *
   * @param index The position at which the memory will be written.
   * @param value The double value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 8.
   */
  public final void putDouble(int index, double value) {
    putLong(index, Double.doubleToRawLongBits(value));
  }

  /**
   * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position
   * in little endian byte order. This method's speed depends on the system's native byte order, and
   * it is possibly slower than {@link #putDouble(int, double)}. For most cases (such as transient
   * storage in memory or serialization for I/O and network), it suffices to know that the byte
   * order in which the value is written is the same as the one in which it is read, and {@link
   * #putDouble(int, double)} is the preferable choice.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger then the buffer
   *     size minus 8.
   */
  public final void putDoubleL(int index, double value) {
    putLongL(index, Double.doubleToRawLongBits(value));
  }

  // -------------------------------------------------------------------------
  //                     Read and Write Methods
  // -------------------------------------------------------------------------

  /** Returns the {@code readerIndex} of this buffer. */
  public int readerIndex() {
    return readerIndex;
  }

  /**
   * Sets the {@code readerIndex} of this buffer.
   *
   * @throws IndexOutOfBoundsException if the specified {@code readerIndex} is less than {@code 0}
   *     or greater than {@code this.size}
   */
  public MemoryBuffer readerIndex(int readerIndex) {
    if (readerIndex < 0 || readerIndex > size) {
      throw new IndexOutOfBoundsException(
          String.format(
              "readerIndex: %d (expected: 0 <= readerIndex <= size(%d))", readerIndex, size));
    }
    this.readerIndex = readerIndex;
    return this;
  }

  public int remaining() {
    return size - readerIndex;
  }

  /** Returns the {@code writerIndex} of this buffer. */
  public int writerIndex() {
    return writerIndex;
  }

  /**
   * Sets the {@code writerIndex} of this buffer.
   *
   * @throws IndexOutOfBoundsException if the specified {@code writerIndex} is less than {@code 0}
   *     or greater than {@code this.size}
   */
  public MemoryBuffer writerIndex(int writerIndex) {
    if (writerIndex < 0 || writerIndex > size) {
      throw new IndexOutOfBoundsException(
          String.format(
              "writerIndex: %d (expected: 0 <= writerIndex <= size(%d))", writerIndex, size));
    }
    this.writerIndex = writerIndex;
    return this;
  }

  public MemoryBuffer writeBoolean(boolean value) {
    grow(1);
    putBoolean(writerIndex, value);
    writerIndex++;
    return this;
  }

  public MemoryBuffer writeByte(byte value) {
    grow(1);
    put(writerIndex, value);
    writerIndex++;
    return this;
  }

  public MemoryBuffer writeChar(char value) {
    grow(2);
    putChar(writerIndex, value);
    writerIndex += 2;
    return this;
  }

  public MemoryBuffer writeShort(short value) {
    grow(2);
    putShort(writerIndex, value);
    writerIndex += 2;
    return this;
  }

  public MemoryBuffer writeInt(int value) {
    grow(4);
    putInt(writerIndex, value);
    writerIndex += 4;
    return this;
  }

  public MemoryBuffer writeLong(long value) {
    grow(8);
    putLong(writerIndex, value);
    writerIndex += 8;
    return this;
  }

  public MemoryBuffer writeFloat(float value) {
    grow(4);
    putFloat(writerIndex, value);
    writerIndex += 4;
    return this;
  }

  public MemoryBuffer writeDouble(double value) {
    grow(8);
    putDouble(writerIndex, value);
    writerIndex += 8;
    return this;
  }

  public MemoryBuffer writeBytes(byte[] bytes) {
    return writeBytes(bytes, 0, bytes.length);
  }

  public MemoryBuffer writeBytes(byte[] bytes, int offset, int length) {
    grow(length);
    put(writerIndex, bytes, offset, length);
    writerIndex += length;
    return this;
  }

  public MemoryBuffer write(ByteBuffer source) {
    return write(source, source.remaining());
  }

  public MemoryBuffer write(ByteBuffer source, int numBytes) {
    grow(numBytes);
    put(writerIndex, source, numBytes);
    writerIndex += numBytes;
    return this;
  }

  /** For off-heap buffer, this will make a heap buffer internally */
  public void grow(int neededSize) {
    ensure(writerIndex + neededSize);
  }

  /** For off-heap buffer, this will make a heap buffer internally */
  public void ensure(int length) {
    if (length > size) {
      byte[] data = new byte[length * 2];
      copyToUnsafe(0, data, BYTE_ARRAY_BASE_OFFSET, size());
      initHeapBuffer(data, 0, data.length);
    }
  }

  public boolean readBoolean() {
    checkReadableBytes(1);
    boolean v = getBoolean(readerIndex);
    readerIndex += 1;
    return v;
  }

  public byte readByte() {
    checkReadableBytes(1);
    byte v = get(readerIndex);
    readerIndex += 1;
    return v;
  }

  public char readChar() {
    checkReadableBytes(2);
    char v = getChar(readerIndex);
    readerIndex += 2;
    return v;
  }

  public short readShort() {
    checkReadableBytes(2);
    short v = getShort(readerIndex);
    readerIndex += 2;
    return v;
  }

  public int readInt() {
    checkReadableBytes(4);
    int v = getInt(readerIndex);
    readerIndex += 4;
    return v;
  }

  public long readLong() {
    checkReadableBytes(8);
    long v = getLong(readerIndex);
    readerIndex += 8;
    return v;
  }

  public float readFloat() {
    checkReadableBytes(4);
    float v = getFloat(readerIndex);
    readerIndex += 4;
    return v;
  }

  public double readDouble() {
    checkReadableBytes(8);
    double v = getDouble(readerIndex);
    readerIndex += 8;
    return v;
  }

  public byte[] readBytes(int length) {
    checkReadableBytes(length);
    byte[] bytes = getBytes(readerIndex, length);
    readerIndex += length;
    return bytes;
  }

  public void readBytes(byte[] dst, int dstIndex, int length) {
    checkReadableBytes(length);
    getBytes(readerIndex, dst, dstIndex, length);
    readerIndex += length;
  }

  public void readBytes(byte[] dst) {
    readBytes(dst, 0, dst.length);
  }

  public void read(ByteBuffer dst) {
    int len = Math.min(dst.remaining(), size - readerIndex);
    checkReadableBytes(len);
    dst.put(sliceAsByteBuffer(readerIndex, len));
    readerIndex += len;
  }

  public void checkReadableBytes(int minimumReadableBytes) {
    if (readerIndex > size - minimumReadableBytes) {
      throw new IndexOutOfBoundsException(
          String.format(
              "readerIndex(%d) + length(%d) exceeds size(%d): %s",
              readerIndex, minimumReadableBytes, writerIndex, this));
    }
  }

  // -------------------------------------------------------------------------
  //                     Bulk Read and Write Methods
  // -------------------------------------------------------------------------

  /**
   * Bulk get method. Copies {@code numBytes} bytes from this memory buffer, starting at position
   * {@code offset} to the target {@code ByteBuffer}. The bytes will be put into the target buffer
   * starting at the buffer's current position. If this method attempts to write more bytes than the
   * target byte buffer has remaining (with respect to {@link ByteBuffer#remaining()}), this method
   * will cause a {@link BufferOverflowException}.
   *
   * @param offset The position where the bytes are started to be read from in this memory buffer.
   * @param target The ByteBuffer to copy the bytes to.
   * @param numBytes The number of bytes to copy.
   * @throws IndexOutOfBoundsException If the offset is invalid, or this buffer does not contain the
   *     given number of bytes (starting from offset), or the target byte buffer does not have
   *     enough space for the bytes.
   * @throws ReadOnlyBufferException If the target buffer is read-only.
   */
  public final void get(int offset, ByteBuffer target, int numBytes) {
    // check the byte array offset and length
    if ((offset | numBytes | (offset + numBytes)) < 0) {
      throw new IndexOutOfBoundsException();
    }

    final int targetOffset = target.position();
    final int remaining = target.remaining();

    if (remaining < numBytes) {
      throw new BufferOverflowException();
    }

    if (target.isDirect()) {
      if (target.isReadOnly()) {
        throw new ReadOnlyBufferException();
      }

      // copy to the target memory directly
      final long targetPointer = Platform.getAddress(target) + targetOffset;
      final long sourcePointer = address + offset;

      if (sourcePointer <= addressLimit - numBytes) {
        UNSAFE.copyMemory(heapMemory, sourcePointer, null, targetPointer, numBytes);
        target.position(targetOffset + numBytes);
      } else if (address > addressLimit) {
        throw new IllegalStateException("Buffer has been freed");
      } else {
        throw new IndexOutOfBoundsException();
      }
    } else if (target.hasArray()) {
      // move directly into the byte array
      get(offset, target.array(), targetOffset + target.arrayOffset(), numBytes);

      // this must be after the get() call to ensue that the byte buffer is not
      // modified in case the call fails
      target.position(targetOffset + numBytes);
    } else {
      // neither heap buffer nor direct buffer
      while (target.hasRemaining()) {
        target.put(get(offset++));
      }
    }
  }

  /**
   * Bulk put method. Copies {@code numBytes} bytes from the given {@code ByteBuffer}, into this
   * memory buffer. The bytes will be read from the target buffer starting at the buffer's current
   * position, and will be written to this memory buffer starting at {@code offset}. If this method
   * attempts to read more bytes than the target byte buffer has remaining (with respect to {@link
   * ByteBuffer#remaining()}), this method will cause a {@link BufferUnderflowException}.
   *
   * @param offset The position where the bytes are started to be written to in this memory buffer.
   * @param source The ByteBuffer to copy the bytes from.
   * @param numBytes The number of bytes to copy.
   * @throws IndexOutOfBoundsException If the offset is invalid, or the source buffer does not
   *     contain the given number of bytes, or this buffer does not have enough space for the bytes
   *     (counting from offset).
   */
  public final void put(int offset, ByteBuffer source, int numBytes) {
    // check the byte array offset and length
    if ((offset | numBytes | (offset + numBytes)) < 0) {
      throw new IndexOutOfBoundsException();
    }

    final int sourceOffset = source.position();
    final int remaining = source.remaining();

    if (remaining < numBytes) {
      throw new BufferUnderflowException();
    }

    if (source.isDirect()) {
      // copy to the target memory directly
      final long sourcePointer = Platform.getAddress(source) + sourceOffset;
      final long targetPointer = address + offset;

      if (targetPointer <= addressLimit - numBytes) {
        UNSAFE.copyMemory(null, sourcePointer, heapMemory, targetPointer, numBytes);
        source.position(sourceOffset + numBytes);
      } else if (address > addressLimit) {
        throw new IllegalStateException("Buffer has been freed");
      } else {
        throw new IndexOutOfBoundsException();
      }
    } else if (source.hasArray()) {
      // move directly into the byte array
      put(offset, source.array(), sourceOffset + source.arrayOffset(), numBytes);

      // this must be after the get() call to ensue that the byte buffer is not
      // modified in case the call fails
      source.position(sourceOffset + numBytes);
    } else {
      // neither heap buffer nor direct buffer
      while (source.hasRemaining()) {
        put(offset++, source.get());
      }
    }
  }

  /**
   * Bulk copy method. Copies {@code numBytes} bytes from this memory buffer, starting at position
   * {@code offset} to the target memory buffer. The bytes will be put into the target buffer
   * starting at position {@code targetOffset}.
   *
   * @param offset The position where the bytes are started to be read from in this memory buffer.
   * @param target The memory buffer to copy the bytes to.
   * @param targetOffset The position in the target memory buffer to copy the chunk to.
   * @param numBytes The number of bytes to copy.
   * @throws IndexOutOfBoundsException If either of the offsets is invalid, or the source buffer
   *     does not contain the given number of bytes (starting from offset), or the target buffer
   *     does not have enough space for the bytes (counting from targetOffset).
   */
  public final void copyTo(int offset, MemoryBuffer target, int targetOffset, int numBytes) {
    final byte[] thisHeapRef = this.heapMemory;
    final byte[] otherHeapRef = target.heapMemory;
    final long thisPointer = this.address + offset;
    final long otherPointer = target.address + targetOffset;

    if ((numBytes | offset | targetOffset) >= 0
        && thisPointer <= this.addressLimit - numBytes
        && otherPointer <= target.addressLimit - numBytes) {
      UNSAFE.copyMemory(thisHeapRef, thisPointer, otherHeapRef, otherPointer, numBytes);
    } else if (this.address > this.addressLimit) {
      throw new IllegalStateException("this memory buffer has been freed.");
    } else if (target.address > target.addressLimit) {
      throw new IllegalStateException("target memory buffer has been freed.");
    } else {
      throw new IndexOutOfBoundsException(
          String.format(
              "offset=%d, targetOffset=%d, numBytes=%d, address=%d, targetAddress=%d",
              offset, targetOffset, numBytes, this.address, target.address));
    }
  }

  public final void copyFrom(int offset, MemoryBuffer source, int sourcePointer, int numBytes) {
    source.copyTo(sourcePointer, this, offset, numBytes);
  }

  /**
   * Bulk copy method. Copies {@code numBytes} bytes to target unsafe object and pointer. NOTE: This
   * is a unsafe method, no check here, please be carefully.
   */
  public final void copyToUnsafe(long offset, Object target, long targetPointer, int numBytes) {
    final long thisPointer = this.address + offset;
    checkArgument(thisPointer + numBytes <= addressLimit);
    UNSAFE.copyMemory(this.heapMemory, thisPointer, target, targetPointer, numBytes);
  }

  /**
   * Bulk copy method. Copies {@code numBytes} bytes from source unsafe object and pointer. NOTE:
   * This is a unsafe method, no check here, please be carefully.
   */
  public final void copyFromUnsafe(long offset, Object source, long sourcePointer, long numBytes) {
    final long thisPointer = this.address + offset;
    checkArgument(thisPointer + numBytes <= addressLimit);
    UNSAFE.copyMemory(source, sourcePointer, this.heapMemory, thisPointer, numBytes);
  }

  /**
   * @return internal byte array if data is on heap and remaining buffer size is equal to internal
   *     byte array size, or create a new byte array which copy remaining data from off-heap
   */
  public byte[] getRemainingBytes() {
    int length = size - readerIndex;
    if (heapMemory != null && size == length) {
      return heapMemory;
    } else {
      return getBytes(readerIndex, length);
    }
  }

  /**
   * @return internal byte array if data is on heap and buffer size is equal to internal byte array
   *     size , or create a new byte array which copy data from off-heap
   */
  public byte[] getAllBytes() {
    if (heapMemory != null && size == heapMemory.length) {
      return heapMemory;
    } else {
      return getBytes(0, size);
    }
  }

  public byte[] getBytes(int index, int length) {
    Preconditions.checkArgument(index + length <= size);
    byte[] data = new byte[length];
    copyToUnsafe(index, data, BYTE_ARRAY_BASE_OFFSET, length);
    return data;
  }

  public void getBytes(int index, byte[] dst, int dstIndex, int length) {
    Preconditions.checkArgument(dstIndex + length <= dst.length);
    Preconditions.checkArgument(index + length <= size);
    copyToUnsafe(index, dst, BYTE_ARRAY_BASE_OFFSET + dstIndex, length);
  }

  /**
   * Point this MemoryBuffer to a new buff and owner (reuse this MemoryBuffer object).
   *
   * @param buffer the new buffer to point to.
   */
  public void pointTo(byte[] buffer) {
    initHeapBuffer(buffer, 0, buffer.length);
    this.offHeapBuffer = null;
  }

  public MemoryBuffer slice(int offset) {
    return slice(offset, size - offset);
  }

  public MemoryBuffer slice(int offset, int length) {
    Preconditions.checkArgument(offset + length <= size);
    MemoryBuffer buffer = cloneReference();
    buffer.address = address + offset;
    buffer.size = length;
    return buffer;
  }

  public ByteBuffer sliceAsByteBuffer() {
    return sliceAsByteBuffer(readerIndex, size - readerIndex);
  }

  public ByteBuffer sliceAsByteBuffer(int offset, int length) {
    Preconditions.checkArgument(offset + length <= size);
    if (heapMemory != null) {
      return ByteBuffer.wrap(heapMemory, (int) (address - BYTE_ARRAY_BASE_OFFSET + offset), length)
          .slice();
    } else {
      ByteBuffer offHeapBuffer = ((MemoryBuffer) this).offHeapBuffer;
      if (offHeapBuffer != null) {
        ByteBuffer duplicate = offHeapBuffer.duplicate();
        int start = (int) (address - Platform.getAddress(duplicate));
        duplicate.position(start + offset);
        duplicate.limit(start + offset + length);
        return duplicate.slice();
      } else {
        return Platform.wrapDirectBuffer(address + offset, length);
      }
    }
  }

  /**
   * Return a new MemoryBuffer with the same buffer and clear the data (reuse the buffer).
   *
   * @return a new MemoryBuffer object.
   */
  public MemoryBuffer cloneReference() {
    if (offHeapBuffer != null) {
      return new MemoryBuffer(offHeapBuffer);
    } else if (heapMemory != null) {
      MemoryBuffer buf = new MemoryBuffer(heapMemory);
      buf.address = address;
      buf.size = size;
      return buf;
    } else {
      return new MemoryBuffer(address, size);
    }
  }

  /**
   * Compares two memory buffer regions.
   *
   * @param buf2 Buffer to compare this buffer with
   * @param offset1 Offset of this buffer to start comparing
   * @param offset2 Offset of buf2 to start comparing
   * @param len Length of the compared memory region
   * @return 0 if equal, -1 if buf1 &lt; buf2, 1 otherwise
   */
  public final int compare(MemoryBuffer buf2, int offset1, int offset2, int len) {
    while (len >= 8) {
      // Since compare is byte-wise, we need to use big endian byte-order.
      long l1 = this.getLongB(offset1);
      long l2 = buf2.getLongB(offset2);

      if (l1 != l2) {
        return (l1 < l2) ^ (l1 < 0) ^ (l2 < 0) ? -1 : 1;
      }

      offset1 += 8;
      offset2 += 8;
      len -= 8;
    }
    while (len > 0) {
      int b1 = this.get(offset1) & 0xff;
      int b2 = buf2.get(offset2) & 0xff;
      int cmp = b1 - b2;
      if (cmp != 0) {
        return cmp;
      }
      offset1++;
      offset2++;
      len--;
    }
    return 0;
  }

  /**
   * Equals two memory buffer regions.
   *
   * @param buf2 Buffer to equal this buffer with
   * @param offset1 Offset of this buffer to start equaling
   * @param offset2 Offset of buf2 to start equaling
   * @param len Length of the equaled memory region
   * @return true if equal, false otherwise
   */
  public final boolean equalTo(MemoryBuffer buf2, int offset1, int offset2, int len) {
    final long pos1 = address + offset1;
    final long pos2 = buf2.address + offset2;
    Preconditions.checkArgument(pos1 < addressLimit);
    Preconditions.checkArgument(pos2 < buf2.addressLimit);
    return Platform.arrayEquals(heapMemory, pos1, buf2.heapMemory, pos2, len);
  }
}
