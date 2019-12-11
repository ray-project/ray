package org.ray.streaming.runtime.transfer;

import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.streaming.runtime.util.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(DataWriter.class);

  private long nativeQueueProducerPtr;
  private byte[][] subscribedQueues;
  transient private List<String> subscribedQueuesStringId;
  private ByteBuffer buffer = ByteBuffer.allocateDirect(0);
  private long bufferAddress;
  {
    ensureBuffer(0);
  }

  public DataWriter(long nativeQueueProducerPtr, byte[][] subscribedQueues) {
    this.nativeQueueProducerPtr = nativeQueueProducerPtr;
    this.subscribedQueues = subscribedQueues;
    this.subscribedQueuesStringId = new ArrayList<>();

    for (byte[] idBytes : subscribedQueues) {
      subscribedQueuesStringId.add(ChannelUtils.qidBytesToString(idBytes));
    }
  }

  /**
   * produce msg into the specified queue
   *
   * @param id   channel id
   * @param item message item data section is specified by [position, limit).
   */
  public void produce(ChannelID id, ByteBuffer item) {
    int size = item.remaining();
    ensureBuffer(size);
    buffer.clear();
    buffer.put(item);
    writeMessageNative(nativeQueueProducerPtr, id.getNativeIDPtr(), bufferAddress, size);
  }

  /**
   * produce msg into the specified queues
   *
   * @param ids   channel ids
   * @param item message item data section is specified by [position, limit).
   *            item doesn't have to be a direct buffer.
   */
  public void produce(Set<ChannelID> ids, ByteBuffer item) {
    int size = item.remaining();
    ensureBuffer(size);
    for (ChannelID id : ids) {
      buffer.clear();
      buffer.put(item.duplicate());
      writeMessageNative(nativeQueueProducerPtr, id.getNativeIDPtr(), bufferAddress, size);
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
   * stop produce to avoid blocking
   */
  public void stop() {
    stopProducerNative(nativeQueueProducerPtr);
  }

  /**
   * close produce to release resource
   */
  public void close() {
    if (nativeQueueProducerPtr == 0) {
      return;
    }
    LOG.info("closing channel producer.");
    closeProducerNative(nativeQueueProducerPtr);
    nativeQueueProducerPtr = 0;
    LOG.info("closing channel producer done.");
  }

  private native long createDataWriterNative(
      long coreWorker,
      byte[][] outputActorIds,
      FunctionDescriptor asyncFunction,
      FunctionDescriptor syncFunction,
      byte[][] outputQueueIds,
      long[] seqIds,
      long queueSize,
      long[] creatorTypes,
      byte[] fbsConfigBytes);

  private native void onTransfer(long handler, byte[] buffer);

  private native byte[] onTransferSync(long handler, byte[] buffer);

  private native long writeMessageNative(long nativeQueueProducerPtr, long nativeIDPtr, long address, int size);

  private native void stopProducerNative(long nativeQueueProducerPtr);

  private native void closeProducerNative(long nativeQueueProducerPtr);

}
