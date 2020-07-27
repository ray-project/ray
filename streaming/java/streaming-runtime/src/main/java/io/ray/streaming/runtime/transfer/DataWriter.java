package io.ray.streaming.runtime.transfer;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.streaming.runtime.config.StreamingWorkerConfig;
import io.ray.streaming.runtime.config.types.TransferChannelType;
import io.ray.streaming.runtime.util.Platform;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataWriter is a wrapper of streaming c++ DataWriter, which sends data
 * to downstream workers
 */
public class DataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(DataWriter.class);

  private long nativeWriterPtr;
  private ByteBuffer buffer = ByteBuffer.allocateDirect(0);
  private long bufferAddress;

  {
    ensureBuffer(0);
  }

  /**
   * @param outputChannels output channels ids
   * @param toActors       downstream output actors
   * @param workerConfig   configuration
   */
  public DataWriter(List<String> outputChannels,
                    List<BaseActorHandle> toActors,
                    StreamingWorkerConfig workerConfig) {
    Preconditions.checkArgument(!outputChannels.isEmpty());
    Preconditions.checkArgument(outputChannels.size() == toActors.size());
    ChannelCreationParametersBuilder initialParameters =
        new ChannelCreationParametersBuilder().buildOutputQueueParameters(outputChannels, toActors);
    byte[][] outputChannelsBytes = outputChannels.stream()
        .map(ChannelId::idStrToBytes).toArray(byte[][]::new);
    long channelSize = workerConfig.transferConfig.channelSize();
    long[] msgIds = new long[outputChannels.size()];
    for (int i = 0; i < outputChannels.size(); i++) {
      msgIds[i] = 0;
    }
    TransferChannelType channelType = workerConfig.transferConfig.channelType();
    boolean isMock = false;
    if (TransferChannelType.MEMORY_CHANNEL == channelType) {
      isMock = true;
    }
    this.nativeWriterPtr = createWriterNative(
        initialParameters,
        outputChannelsBytes,
        msgIds,
        channelSize,
        ChannelUtils.toNativeConf(workerConfig),
        isMock
    );
    LOG.info("Create DataWriter succeed for worker: {}.",
        workerConfig.workerInternalConfig.workerName());
  }

  /**
   * Write msg into the specified channel
   *
   * @param id   channel id
   * @param item message item data section is specified by [position, limit).
   */
  public void write(ChannelId id, ByteBuffer item) {
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
  public void write(Set<ChannelId> ids, ByteBuffer item) {
    int size = item.remaining();
    ensureBuffer(size);
    for (ChannelId id : ids) {
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
    LOG.info("Closing data writer.");
    closeWriterNative(nativeWriterPtr);
    nativeWriterPtr = 0;
    LOG.info("Finish closing data writer.");
  }

  private static native long createWriterNative(
      ChannelCreationParametersBuilder initialParameters,
      byte[][] outputQueueIds,
      long[] msgIds,
      long channelSize,
      byte[] confBytes,
      boolean isMock);

  private native long writeMessageNative(
      long nativeQueueProducerPtr, long nativeIdPtr, long address, int size);

  private native void stopWriterNative(long nativeQueueProducerPtr);

  private native void closeWriterNative(long nativeQueueProducerPtr);

}
