package io.ray.streaming.runtime.transfer;

import com.google.common.base.FinalizablePhantomReference;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;
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

  private final byte[] bytes;
  private final String strId;
  private final ByteBuffer buffer;
  private final long address;
  private final long nativeIdPtr;

  private ChannelID(String strId, byte[] idBytes) {
    this.strId = strId;
    this.bytes = idBytes;
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(ID_LENGTH);
    directBuffer.put(bytes);
    directBuffer.rewind();
    this.buffer = directBuffer;
    this.address = ((DirectBuffer) (buffer)).address();
    long nativeIdPtr = 0;
    nativeIdPtr = createNativeID(address);
    this.nativeIdPtr = nativeIdPtr;
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

  public long getNativeIdPtr() {
    if (nativeIdPtr == 0) {
      throw new IllegalStateException("native ID not available");
    }
    return nativeIdPtr;
  }

  @Override
  public String toString() {
    return strId;
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
    return strId.equals(that.strId);
  }

  @Override
  public int hashCode() {
    return strId.hashCode();
  }

  private static native long createNativeID(long idAddress);

  private static native void destroyNativeID(long nativeIdPtr);

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
    long nativeIdPtr = id.nativeIdPtr;
    if (nativeIdPtr != 0) {
      Reference<ChannelID> reference =
          new FinalizablePhantomReference<ChannelID>(id, REFERENCE_QUEUE) {
            @Override
            public void finalizeReferent() {
              destroyNativeID(nativeIdPtr);
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
      |    Head    | Timestamp | Empty | From  |  To    |
      | 8 bytes    |  4bytes   | 4bytes| 2bytes| 2bytes |
    */
    // The Guava Preconditions checks take error message template
    // strings that look similar to format strings but only accept %s
    // as a placeholder. For more details check here:
    // https://errorprone.info/bugpattern/PreconditionsInvalidPlaceholder
    Preconditions.checkArgument(fromTaskId < Short.MAX_VALUE,
        "fromTaskId %s is larger than %s", Integer.toString(fromTaskId), Integer.toString(Short.MAX_VALUE));
    Preconditions.checkArgument(toTaskId < Short.MAX_VALUE,
        "toTaskId %s is larger than %s", Integer.toString(fromTaskId), Integer.toString(Short.MAX_VALUE));
    byte[] channelName = new byte[20];

    for (int i = 11; i >= 8; i--) {
      channelName[i] = (byte) (ts & 0xff);
      ts >>= 8;
    }

    channelName[16] = (byte) ((fromTaskId & 0xffff) >> 8);
    channelName[17] = (byte) (fromTaskId & 0xff);
    channelName[18] = (byte) ((toTaskId & 0xffff) >> 8);
    channelName[19] = (byte) (toTaskId & 0xff);

    return ChannelID.idBytesToStr(channelName);
  }

  /**
   * @param id hex string representation of channel id
   * @return bytes representation of channel id
   */
  static byte[] idStrToBytes(String id) {
    byte[] idBytes = BaseEncoding.base16().decode(id.toUpperCase());
    assert idBytes.length == ChannelID.ID_LENGTH;
    return idBytes;
  }

  /**
   * @param id bytes representation of channel id
   * @return hex string representation of channel id
   */
  static String idBytesToStr(byte[] id) {
    assert id.length == ChannelID.ID_LENGTH;
    return BaseEncoding.base16().encode(id).toLowerCase();
  }

}

