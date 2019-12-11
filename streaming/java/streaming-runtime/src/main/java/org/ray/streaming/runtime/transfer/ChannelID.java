package org.ray.streaming.runtime.transfer;

import com.google.common.base.FinalizablePhantomReference;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.collect.Sets;
import sun.nio.ch.DirectBuffer;

import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.util.Set;

public class ChannelID {
  public static final int ID_LENGTH = 20;
  private static final FinalizableReferenceQueue REFERENCE_QUEUE = new FinalizableReferenceQueue();
  // This ensures that the FinalizablePhantomReference itself is not garbage-collected.
  private static final Set<Reference<?>> references = Sets.newConcurrentHashSet();

  static {
  }

  private final byte[] bytes;
  private final String strID;
  private final ByteBuffer buffer;
  private final long address;
  private final long nativeIDPtr;

  private ChannelID(String strID, byte[] idBytes) {
    this.strID = strID;
    this.bytes = idBytes;
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(ID_LENGTH);
    directBuffer.put(bytes);
    directBuffer.rewind();
    this.buffer = directBuffer;
    this.address = ((DirectBuffer)(buffer)).address();
    long nativeIDPtr = 0;
    nativeIDPtr = createNativeID(address);
    this.nativeIDPtr = nativeIDPtr;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public long getAddress() {
    return address;
  }

  public long getNativeIDPtr() {
    if (nativeIDPtr == 0) {
      throw new IllegalStateException("native ID not available");
    }
    return nativeIDPtr;
  }

  @Override
  public String toString() {
    return strID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChannelID that = (ChannelID) o;
    return strID.equals(that.strID);
  }

  @Override
  public int hashCode() {
    return strID.hashCode();
  }

  private static native long createNativeID(long idAddress);

  private static native void destroyNativeID(long nativeIDPtr);

  public static ChannelID from(String id) {
    return from(id, ChannelUtils.qidStrToBytes(id));
  }

  public static ChannelID from(byte[] idBytes) {
    return from(ChannelUtils.qidBytesToString(idBytes), idBytes);
  }

  private static ChannelID from(String strID, byte[] idBytes) {
    ChannelID id = new ChannelID(strID, idBytes);
    long nativeIDPtr = id.nativeIDPtr;
    if (nativeIDPtr != 0) {
      Reference<ChannelID> reference = new FinalizablePhantomReference<ChannelID>(id, REFERENCE_QUEUE) {
        @Override
        public void finalizeReferent() {
          destroyNativeID(nativeIDPtr);
          references.remove(this);
        }
      };
      references.add(reference);
    }
    return id;
  }

}

