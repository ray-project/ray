package io.ray.runtime.io;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Preconditions;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;

/**
 * A class for operations on memory managed by Ray. The buffer may be backed by heap memory (byte
 * array) or by off-heap memory. If the memory is off-heap memory, it will be the memory allocated
 * by ray object store. Note that the buffer can auto grow on write operations and change into a
 * heap buffer when growing.
 *
 * <p>This class is based on org.apache.flink.core.memory.MemorySegment and
 * org.apache.arrow.memory.ArrowBuf, we add this class mainly for:
 *
 * <ul>
 *   <li>read/write data into a chunk of direct memory.
 *   <li>additional binary compare, swap, and copy methods.
 *   <li>little-endian access.
 *   <li>independent read/write index.
 * </ul>
 *
 * <p>Note that this class is designed to final so that all the methods in this class can be inlined
 * by the just-in-time compiler.
 */
public final class MemoryBuffer {
  // The unsafe handle for transparent memory copied (heap / off-heap).
  private static final sun.misc.Unsafe UNSAFE = Platform.UNSAFE;
  // The beginning of the byte array contents, relative to the byte array object.
  private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
  // Constant that flags the byte order. Because this is a boolean constant, the JIT compiler can
  // use this well to aggressively eliminate the non-applicable code paths.
  private static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

  // If the data in on the heap, `heapMemory` will be non-null, and its' the object relative to
  // which we access the memory.
  // If we have this buffer, we must never void this reference, or the memory buffer will point
  // to undefined addresses outside the heap and may in out-of-order execution cases cause
  // buffer faults.
  private byte[] heapMemory;
  // If the data is off the heap, `offHeapBuffer` will be non-null, and it's the direct byte buffer
  // that allocated on the off-heap memory.
  // This memory buffer holds a reference to that buffer, so as long as this memory buffer lives,
  // the memory will not be released.
  private ByteBuffer offHeapBuffer;
  // The readable/writeable range is [address, addressLimit).
  // If the data in on the heap, this is the relative offset to the `heapMemory` byte array.
  // If the data is off the heap, this is the absolute memory address.
  private long address;
  // The address one byte after the last addressable byte, i.e. `address + size` while the
  // buffer is not disposed.
  private long addressLimit;
  // The size in bytes of the memory buffer.
  private int size;
  private int readerIndex;
  private int writerIndex;

  /**
   * Creates a new memory buffer that represents the memory of the byte array.
   *
   * @param buffer The byte array whose memory is represented by this memory buffer.
   * @param offset The offset of the sub array to be used; must be non-negative and no larger than
   *     <tt>array.length</tt>.
   * @param length buffer size
   */
  private MemoryBuffer(byte[] buffer, int offset, int length) {
    Preconditions.checkArgument(offset >= 0 && length >= 0);
    if (offset + length > buffer.length) {
      throw new IllegalArgumentException(
          String.format("%d exceeds buffer size %d", offset + length, buffer.length));
    }
    initHeapBuffer(buffer, offset, length);
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
   * Creates a new memory buffer that represents the native memory at the absolute address given by
   * the pointer.
   *
   * @param offHeapAddress The address of the memory represented by this memory buffer.
   * @param size The size of this memory buffer.
   * @param offHeapBuffer The byte buffer whose memory is represented by this memory buffer which
   *     may be null if the memory is not allocated by `DirectByteBuffer`. Hold this buffer to avoid
   *     the memory being released.
   */
  private MemoryBuffer(long offHeapAddress, int size, ByteBuffer offHeapBuffer) {
    this.offHeapBuffer = offHeapBuffer;
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

  public byte get(int index) {
    final long pos = address + index;
    if (index >= 0 && pos < addressLimit) {
      return UNSAFE.getByte(heapMemory, pos);
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  public void get(int index, byte[] dst) {
    get(index, dst, 0, dst.length);
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
  public void get(int index, byte[] dst, int offset, int length) {
    // check the byte array offset and length and the status
    if ((offset | length | (offset + length) | (dst.length - (offset + length))) < 0) {
      throw new IndexOutOfBoundsException();
    }
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - length) {
      final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
      Platform.copyMemory(heapMemory, pos, dst, arrayAddress, length);
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

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
  public void get(int offset, ByteBuffer target, int numBytes) {
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
        Platform.copyMemory(heapMemory, sourcePointer, null, targetPointer, numBytes);
        target.position(targetOffset + numBytes);
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
   *     contain the given number of bytes, or this buffer does not have enough space for the
   *     bytes(counting from offset).
   */
  public void put(int offset, ByteBuffer source, int numBytes) {
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
        Platform.copyMemory(null, sourcePointer, heapMemory, targetPointer, numBytes);
        source.position(sourceOffset + numBytes);
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

  public void put(int index, byte b) {
    final long pos = address + index;
    if (index >= 0 && pos < addressLimit) {
      UNSAFE.putByte(heapMemory, pos, b);
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  public void put(int index, byte[] src) {
    put(index, src, 0, src.length);
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
  public void put(int index, byte[] src, int offset, int length) {
    // check the byte array offset and length
    if ((offset | length | (offset + length) | (src.length - (offset + length))) < 0) {
      throw new IndexOutOfBoundsException();
    }
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - length) {
      final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
      Platform.copyMemory(src, arrayAddress, heapMemory, pos, length);
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  public boolean getBoolean(int index) {
    return get(index) != 0;
  }

  public void putBoolean(int index, boolean value) {
    put(index, (byte) (value ? 1 : 0));
  }

  public char getCharN(int index) {
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

  public char getChar(int index) {
    if (LITTLE_ENDIAN) {
      return getCharN(index);
    } else {
      return Character.reverseBytes(getChar(index));
    }
  }

  public void putCharN(int index, char value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      UNSAFE.putChar(heapMemory, pos, value);
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  public void putChar(int index, char value) {
    if (LITTLE_ENDIAN) {
      putCharN(index, value);
    } else {
      putChar(index, Character.reverseBytes(value));
    }
  }

  public short getShortN(int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      return UNSAFE.getShort(heapMemory, pos);
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  public short getShort(int index) {
    if (LITTLE_ENDIAN) {
      return getShortN(index);
    } else {
      return Short.reverseBytes(getShort(index));
    }
  }

  public void putShortN(int index, short value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      UNSAFE.putShort(heapMemory, pos, value);
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  public void putShort(int index, short value) {
    if (LITTLE_ENDIAN) {
      putShortN(index, value);
    } else {
      putShort(index, Short.reverseBytes(value));
    }
  }

  public int getIntN(int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 4) {
      return UNSAFE.getInt(heapMemory, pos);
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  public int getInt(int index) {
    if (LITTLE_ENDIAN) {
      return getIntN(index);
    } else {
      return Integer.reverseBytes(getInt(index));
    }
  }

  public void putIntN(int index, int value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 4) {
      UNSAFE.putInt(heapMemory, pos, value);
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  public void putInt(int index, int value) {
    if (LITTLE_ENDIAN) {
      putIntN(index, value);
    } else {
      putInt(index, Integer.reverseBytes(value));
    }
  }

  public long getLongN(int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 8) {
      return UNSAFE.getLong(heapMemory, pos);
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  public long getLong(int index) {
    if (LITTLE_ENDIAN) {
      return getLongN(index);
    } else {
      return Long.reverseBytes(getLong(index));
    }
  }

  public long getLongB(int index) {
    if (LITTLE_ENDIAN) {
      return Long.reverseBytes(getLongN(index));
    } else {
      return getLong(index);
    }
  }

  public void putLongN(int index, long value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 8) {
      UNSAFE.putLong(heapMemory, pos, value);
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  public void putLong(int index, long value) {
    if (LITTLE_ENDIAN) {
      putLongN(index, value);
    } else {
      putLong(index, Long.reverseBytes(value));
    }
  }

  public void putLongB(int index, long value) {
    if (LITTLE_ENDIAN) {
      putLongN(index, Long.reverseBytes(value));
    } else {
      putLong(index, value);
    }
  }

  public float getFloatN(int index) {
    return Float.intBitsToFloat(getIntN(index));
  }

  public float getFloat(int index) {
    return Float.intBitsToFloat(getInt(index));
  }

  public void putFloatN(int index, float value) {
    putIntN(index, Float.floatToRawIntBits(value));
  }

  public void putFloat(int index, float value) {
    putInt(index, Float.floatToRawIntBits(value));
  }

  public double getDoubleN(int index) {
    return Double.longBitsToDouble(getLongN(index));
  }

  public double getDouble(int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  public void putDoubleN(int index, double value) {
    putLongN(index, Double.doubleToRawLongBits(value));
  }

  public void putDouble(int index, double value) {
    putLong(index, Double.doubleToRawLongBits(value));
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
  public void writerIndex(int writerIndex) {
    if (writerIndex < 0 || writerIndex > size) {
      throw new IndexOutOfBoundsException(
          String.format(
              "writerIndex: %d (expected: 0 <= writerIndex <= size(%d))", writerIndex, size));
    }
    this.writerIndex = writerIndex;
  }

  public void writeBoolean(boolean value) {
    grow(1);
    putBoolean(writerIndex, value);
    writerIndex++;
  }

  public void writeByte(byte value) {
    grow(1);
    put(writerIndex, value);
    writerIndex++;
  }

  public void writeChar(char value) {
    grow(2);
    putChar(writerIndex, value);
    writerIndex += 2;
  }

  public void writeShort(short value) {
    grow(2);
    putShort(writerIndex, value);
    writerIndex += 2;
  }

  public void writeInt(int value) {
    grow(4);
    putInt(writerIndex, value);
    writerIndex += 4;
  }

  public void writeLong(long value) {
    grow(8);
    putLong(writerIndex, value);
    writerIndex += 8;
  }

  public void writeFloat(float value) {
    grow(4);
    putFloat(writerIndex, value);
    writerIndex += 4;
  }

  public void writeDouble(double value) {
    grow(8);
    putDouble(writerIndex, value);
    writerIndex += 8;
  }

  public void writeBytes(byte[] bytes) {
    writeBytes(bytes, 0, bytes.length);
  }

  public void writeBytes(byte[] bytes, int offset, int length) {
    grow(length);
    put(writerIndex, bytes, offset, length);
    writerIndex += length;
  }

  public void write(ByteBuffer source) {
    write(source, source.remaining());
  }

  public void write(ByteBuffer source, int numBytes) {
    grow(numBytes);
    put(writerIndex, source, numBytes);
    writerIndex += numBytes;
  }

  /** For off-heap buffer, this will make a heap buffer internally. */
  public void grow(int neededSize) {
    ensure(writerIndex + neededSize);
  }

  /** For off-heap buffer, this will make a heap buffer internally. */
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

  /**
   * Bulk copy method. Copies {@code numBytes} bytes to target unsafe object and pointer. NOTE: This
   * is a unsafe method, no check here, please be carefully.
   */
  public void copyToUnsafe(long offset, Object target, long targetPointer, int numBytes) {
    final long thisPointer = this.address + offset;
    checkArgument(thisPointer + numBytes <= addressLimit);
    Platform.copyMemory(this.heapMemory, thisPointer, target, targetPointer, numBytes);
  }

  /**
   * Bulk copy method. Copies {@code numBytes} bytes from source unsafe object and pointer. NOTE:
   * This is an unsafe method, no check here, please be carefully.
   */
  public void copyFromUnsafe(long offset, Object source, long sourcePointer, long numBytes) {
    final long thisPointer = this.address + offset;
    checkArgument(thisPointer + numBytes <= addressLimit);
    Platform.copyMemory(source, sourcePointer, this.heapMemory, thisPointer, numBytes);
  }

  /**
   * Returns internal byte array if data is on heap and remaining buffer size is equal to internal
   * byte array size, or create a new byte array which copy remaining data from off-heap.
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
   * Returns internal byte array if data is on heap and buffer size is equal to internal byte array
   * size , or create a new byte array which copy data from off-heap.
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
      return ByteBuffer.wrap(heapMemory, (int) (address - BYTE_ARRAY_BASE_OFFSET + offset), length);
    } else {
      ByteBuffer offHeapBuffer = this.offHeapBuffer;
      if (offHeapBuffer != null) {
        ByteBuffer duplicate = offHeapBuffer.duplicate();
        int start = (int) (address - Platform.getAddress(duplicate));
        duplicate.position(start + offset);
        duplicate.limit(start + offset + length);
        return duplicate.slice();
      } else {
        return Platform.createDirectByteBufferFromNativeAddress(address + offset, length);
      }
    }
  }

  public boolean equalTo(MemoryBuffer buf2, int offset1, int offset2, int len) {
    final long pos1 = address + offset1;
    final long pos2 = buf2.address + offset2;
    Preconditions.checkArgument(pos1 < addressLimit);
    Preconditions.checkArgument(pos2 < buf2.addressLimit);
    return Platform.arrayEquals(heapMemory, pos1, buf2.heapMemory, pos2, len);
  }

  /**
   * Return a new MemoryBuffer with the same buffer and clear the data (reuse the buffer).
   *
   * @return a new MemoryBuffer object.
   */
  public MemoryBuffer cloneReference() {
    if (heapMemory != null) {
      MemoryBuffer buf = new MemoryBuffer(heapMemory, 0, heapMemory.length);
      buf.address = address;
      buf.size = size;
      return buf;
    } else {
      return new MemoryBuffer(address, size, offHeapBuffer);
    }
  }

  /** Creates a new memory buffer that targets to the given heap memory region. */
  public static MemoryBuffer fromByteArray(byte[] buffer, int offset, int length) {
    return new MemoryBuffer(buffer, offset, length);
  }

  /** Creates a new memory buffer that targets to the given heap memory region. */
  public static MemoryBuffer fromByteArray(byte[] buffer) {
    return new MemoryBuffer(buffer, 0, buffer.length);
  }

  /**
   * Creates a new memory buffer that represents the memory backing the given byte buffer section of
   * {@code [buffer.position(), buffer.limit())}. The buffer will change into a heap buffer
   * automatically if not enough.
   *
   * @param buffer a direct buffer or heap buffer
   */
  public static MemoryBuffer fromByteBuffer(ByteBuffer buffer) {
    if (buffer.isDirect()) {
      return new MemoryBuffer(
          Platform.getAddress(buffer) + buffer.position(), buffer.remaining(), buffer);
    } else {
      int offset = buffer.arrayOffset() + buffer.position();
      return new MemoryBuffer(buffer.array(), offset, buffer.remaining());
    }
  }

  /**
   * Creates a new memory buffer that represents the provided native memory. The buffer will change
   * into a heap buffer automatically if not enough.
   */
  public static MemoryBuffer fromNativeAddress(long address, int size) {
    return new MemoryBuffer(address, size, null);
  }

  /**
   * Create a heap buffer of specified initial size. The buffer will grow automatically if not
   * enough.
   */
  public static MemoryBuffer newHeapBuffer(int initialSize) {
    return fromByteArray(new byte[initialSize]);
  }
}
