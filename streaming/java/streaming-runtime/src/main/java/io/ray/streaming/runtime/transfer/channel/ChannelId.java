package io.ray.streaming.runtime.transfer.channel;

import com.google.common.base.FinalizablePhantomReference;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import io.ray.api.id.ObjectId;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;
import sun.nio.ch.DirectBuffer;

/**
 * ChannelID is used to identify a transfer channel between a upstream worker and downstream worker.
 */
public class ChannelId {

  public static final int ID_LENGTH = ObjectId.LENGTH;
  private static final FinalizableReferenceQueue REFERENCE_QUEUE = new FinalizableReferenceQueue();
  // This ensures that the FinalizablePhantomReference itself is not garbage-collected.
  private static final Set<Reference<?>> references = Sets.newConcurrentHashSet();

  private final byte[] bytes;
  private final String strId;
  private final ByteBuffer buffer;
  private final long address;
  private final long nativeIdPtr;

  private ChannelId(String strId, byte[] idBytes) {
    this.strId = strId;
    this.bytes = idBytes;
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(ID_LENGTH);
    directBuffer.put(bytes);
    directBuffer.rewind();
    this.buffer = directBuffer;
    this.address = ((DirectBuffer) (buffer)).address();
    long nativeIdPtr = 0;
    nativeIdPtr = createNativeId(address);
    this.nativeIdPtr = nativeIdPtr;
  }

  private static native long createNativeId(long idAddress);

  private static native void destroyNativeId(long nativeIdPtr);

  /** @param id hex string representation of channel id */
  public static ChannelId from(String id) {
    return from(id, ChannelId.idStrToBytes(id));
  }

  /** @param idBytes bytes representation of channel id */
  public static ChannelId from(byte[] idBytes) {
    return from(idBytesToStr(idBytes), idBytes);
  }

  private static ChannelId from(String strID, byte[] idBytes) {
    ChannelId id = new ChannelId(strID, idBytes);
    long nativeIdPtr = id.nativeIdPtr;
    if (nativeIdPtr != 0) {
      Reference<ChannelId> reference =
          new FinalizablePhantomReference<ChannelId>(id, REFERENCE_QUEUE) {
            @Override
            public void finalizeReferent() {
              destroyNativeId(nativeIdPtr);
              references.remove(this);
            }
          };
      references.add(reference);
    }
    return id;
  }

  /** Returns a random channel id string */
  public static String genRandomIdStr() {
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    for (int i = 0; i < ChannelId.ID_LENGTH * 2; ++i) {
      sb.append((char) (random.nextInt(6) + 'A'));
    }
    return sb.toString();
  }

  /**
   * Generate channel name, which will be {@link ChannelId#ID_LENGTH} character
   *
   * @param fromTaskId upstream task id
   * @param toTaskId downstream task id Returns channel name
   */
  public static String genIdStr(int fromTaskId, int toTaskId, long ts) {
    /*
      |    Head    | Timestamp | Empty | From  |  To    | padding |
      | 8 bytes    |  4bytes   | 4bytes| 2bytes| 2bytes |         |
    */
    Preconditions.checkArgument(
        fromTaskId < Short.MAX_VALUE,
        "fromTaskId %s is larger than %s",
        fromTaskId,
        Short.MAX_VALUE);
    Preconditions.checkArgument(
        toTaskId < Short.MAX_VALUE, "toTaskId %s is larger than %s", fromTaskId, Short.MAX_VALUE);
    byte[] channelName = new byte[ID_LENGTH];

    for (int i = 11; i >= 8; i--) {
      channelName[i] = (byte) (ts & 0xff);
      ts >>= 8;
    }

    channelName[16] = (byte) ((fromTaskId & 0xffff) >> 8);
    channelName[17] = (byte) (fromTaskId & 0xff);
    channelName[18] = (byte) ((toTaskId & 0xffff) >> 8);
    channelName[19] = (byte) (toTaskId & 0xff);

    return ChannelId.idBytesToStr(channelName);
  }

  /**
   * @param id hex string representation of channel id Returns bytes representation of channel id
   */
  public static byte[] idStrToBytes(String id) {
    byte[] idBytes = BaseEncoding.base16().decode(id.toUpperCase());
    assert idBytes.length == ChannelId.ID_LENGTH;
    return idBytes;
  }

  /**
   * @param id bytes representation of channel id Returns hex string representation of channel id
   */
  public static String idBytesToStr(byte[] id) {
    assert id.length == ChannelId.ID_LENGTH;
    return BaseEncoding.base16().encode(id).toLowerCase();
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
    ChannelId that = (ChannelId) o;
    return strId.equals(that.strId);
  }

  @Override
  public int hashCode() {
    return strId.hashCode();
  }
}
