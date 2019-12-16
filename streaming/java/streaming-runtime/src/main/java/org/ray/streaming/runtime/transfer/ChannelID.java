package org.ray.streaming.runtime.transfer;

import com.google.common.base.Preconditions;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;

import org.ray.runtime.RayNativeRuntime;
import org.ray.streaming.runtime.util.JniUtils;

import com.google.common.base.FinalizablePhantomReference;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.collect.Sets;

import sun.nio.ch.DirectBuffer;

/**
 * ChannelID is used to identify a transfer channel between a upstream worker
 * and downstream worker.
 */
public class ChannelID {
  public static final int ID_LENGTH = 20;
  private static final FinalizableReferenceQueue REFERENCE_QUEUE = new FinalizableReferenceQueue();
  // This ensures that the FinalizablePhantomReference itself is not garbage-collected.
  private static final Set<Reference<?>> references = Sets.newConcurrentHashSet();

  static {
    // load core_worker_library_java before load streaming_java
    try {
      Class.forName(RayNativeRuntime.class.getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    JniUtils.loadLibrary("streaming_java");
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
    this.address = ((DirectBuffer) (buffer)).address();
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

  /**
   * @param id hex string representation of channel id
   */
  public static ChannelID from(String id) {
    return from(id, ChannelID.idStrToBytes(id));
  }

  /**
   * @param idBytes bytes representation of channel id
   */
  public static ChannelID from(byte[] idBytes) {
    return from(idBytesToStr(idBytes), idBytes);
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

  /**
   * @return a random channel id string
   */
  public static String genRandomIdStr() {
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    for (int i = 0; i < ChannelID.ID_LENGTH * 2; ++i) {
      sb.append((char) (random.nextInt(6) + 'A'));
    }
    return sb.toString();
  }

  /**
   * Generate channel name, which will be 20 character
   *
   * @param fromTaskId upstream task id
   * @param toTaskId   downstream task id
   * @return channel name
   */
  public static String genIdStr(int fromTaskId, int toTaskId, long ts) {
    /*
      | Queue Head | Timestamp | Empty | From  |  To    |
      | 8 bytes    |  4bytes   | 4bytes| 2bytes| 2bytes |
    */
    Preconditions.checkArgument(fromTaskId < Short.MAX_VALUE,
        "fromTaskId %d is larger than %d", fromTaskId, Short.MAX_VALUE);
    Preconditions.checkArgument(toTaskId < Short.MAX_VALUE,
        "toTaskId %d is larger than %d", fromTaskId, Short.MAX_VALUE);
    byte[] queueName = new byte[20];

    for (int i = 11; i >= 8; i--) {
      queueName[i] = (byte) (ts & 0xff);
      ts >>= 8;
    }

    queueName[16] = (byte) ((fromTaskId & 0xffff) >> 8);
    queueName[17] = (byte) (fromTaskId & 0xff);
    queueName[18] = (byte) ((toTaskId & 0xffff) >> 8);
    queueName[19] = (byte) (toTaskId & 0xff);

    return ChannelID.idBytesToStr(queueName);
  }

  /**
   * @param id hex string representation of channel id
   * @return bytes representation of channel id
   */
  static byte[] idStrToBytes(String id) {
    byte[] qidBytes = DatatypeConverter.parseHexBinary(id.toUpperCase());
    assert qidBytes.length == ChannelID.ID_LENGTH;
    return qidBytes;
  }

  /**
   * @param qid bytes representation of channel id
   * @return hex string representation of channel id
   */
  static String idBytesToStr(byte[] qid) {
    assert qid.length == ChannelID.ID_LENGTH;
    return DatatypeConverter.printHexBinary(qid).toLowerCase();
  }

}

