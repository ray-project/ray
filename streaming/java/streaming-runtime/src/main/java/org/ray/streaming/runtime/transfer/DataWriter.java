package org.ray.streaming.runtime.transfer;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.ray.api.id.ActorId;
import org.ray.streaming.runtime.util.Platform;
import org.ray.streaming.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataWriter is a wrapper of streaming c++ DataWriter, which sends data
 * to downstream workers
 */
public class DataWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataWriter.class);

  private long nativeWriterPtr;
  private ByteBuffer buffer = ByteBuffer.allocateDirect(0);
  private long bufferAddress;

  {
    ensureBuffer(0);
  }

  /**
   * @param outputChannels output channels ids
   * @param toActors       downstream output actors
   * @param conf           configuration
   */
  public DataWriter(List<String> outputChannels,
                    List<ActorId> toActors,
                    Map<String, String> conf) {
    Preconditions.checkArgument(!outputChannels.isEmpty());
    Preconditions.checkArgument(outputChannels.size() == toActors.size());
    byte[][] outputChannelsBytes = outputChannels.stream()
        .map(ChannelID::idStrToBytes).toArray(byte[][]::new);
    byte[][] toActorsBytes = toActors.stream()
        .map(ActorId::getBytes).toArray(byte[][]::new);
    long channelSize = Long.parseLong(
        conf.getOrDefault(Config.CHANNEL_SIZE, Config.CHANNEL_SIZE_DEFAULT));
    long[] msgIds = new long[outputChannels.size()];
    for (int i = 0; i < outputChannels.size(); i++) {
      msgIds[i] = 0;
    }
    String channelType = conf.getOrDefault(Config.CHANNEL_TYPE, Config.DEFAULT_CHANNEL_TYPE);
    boolean isMock = false;
    if (Config.MEMORY_CHANNEL.equals(channelType)) {
      isMock = true;
    }
    this.nativeWriterPtr = createWriterNative(
        outputChannelsBytes,
        toActorsBytes,
        msgIds,
        channelSize,
        ChannelUtils.toNativeConf(conf),
        isMock
    );
    LOGGER.info("create DataWriter succeed");
  }

  /**
   * Write msg into the specified channel
   *
   * @param id   channel id
   * @param item message item data section is specified by [position, limit).
   */
  public void write(ChannelID id, ByteBuffer item) {
    int size = item.remaining();
    ensureBuffer(size);
    buffer.clear();
    buffer.put(item);
    writeMessageNative(nativeWriterPtr, id.getNativeIdPtr(), bufferAddress, size);
  }

  /**
   * Write msg into the specified channels
   *
   * @param ids  channel ids
   * @param item message item data section is specified by [position, limit).
   *             item doesn't have to be a direct buffer.
   */
  public void write(Set<ChannelID> ids, ByteBuffer item) {
    int size = item.remaining();
    ensureBuffer(size);
    for (ChannelID id : ids) {
      buffer.clear();
      buffer.put(item.duplicate());
      writeMessageNative(nativeWriterPtr, id.getNativeIdPtr(), bufferAddress, size);
    }
  }

  private void ensureBuffer(int size) {
    if (buffer.capacity() < size) {
      buffer = ByteBuffer.allocateDirect(size);
      buffer.order(ByteOrder.nativeOrder());
      bufferAddress = Platform.getAddress(buffer);
    }
  }

  /**
   * stop writer
   */
  public void stop() {
    stopWriterNative(nativeWriterPtr);
  }

  /**
   * close writer to release resources
   */
  public void close() {
    if (nativeWriterPtr == 0) {
      return;
    }
    LOGGER.info("closing data writer.");
    closeWriterNative(nativeWriterPtr);
    nativeWriterPtr = 0;
    LOGGER.info("closing data writer done.");
  }

  private static native long createWriterNative(
      byte[][] outputQueueIds,
      byte[][] outputActorIds,
      long[] msgIds,
      long channelSize,
      byte[] confBytes,
      boolean isMock);

  private native long writeMessageNative(
      long nativeQueueProducerPtr, long nativeIdPtr, long address, int size);

  private native void stopWriterNative(long nativeQueueProducerPtr);

  private native void closeWriterNative(long nativeQueueProducerPtr);

}
