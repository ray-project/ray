package org.ray.streaming.queue.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.ray.streaming.queue.QueueID;
import org.ray.streaming.queue.QueueProducer;
import org.ray.streaming.queue.QueueUtils;
import org.ray.streaming.util.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueProducerImpl implements QueueProducer {

  private static final Logger LOG = LoggerFactory.getLogger(QueueProducerImpl.class);

  private long nativeQueueProducerPtr;
  private byte[][] subscribedQueues;
  transient private List<String> subscribedQueuesStringId;
  private ByteBuffer buffer = ByteBuffer.allocateDirect(0);
  private long bufferAddress;
  {
    ensureBuffer(0);
  }

  public QueueProducerImpl(long nativeQueueProducerPtr, byte[][] subscribedQueues) {
    this.nativeQueueProducerPtr = nativeQueueProducerPtr;
    this.subscribedQueues = subscribedQueues;
    this.subscribedQueuesStringId = new ArrayList<>();

    for (byte[] qidByte : subscribedQueues) {
      subscribedQueuesStringId.add(QueueUtils.qidBytesToString(qidByte));
    }
  }

  @Override
  public void produce(QueueID qid, ByteBuffer item) {
    int size = item.remaining();
    ensureBuffer(size);
    buffer.clear();
    buffer.put(item);
    writeMessageNative(nativeQueueProducerPtr, qid.getNativeIDPtr(), bufferAddress, size);
  }

  @Override
  public void produce(Set<QueueID> ids, ByteBuffer item) {
    int size = item.remaining();
    ensureBuffer(size);
    for (QueueID id : ids) {
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

  @Override
  public void stop() {
    stopProducerNative(nativeQueueProducerPtr);
  }

  @Override
  public void close() {
    if (nativeQueueProducerPtr == 0) {
      return;
    }
    LOG.info("closing queue producer.");
    closeProducerNative(nativeQueueProducerPtr);
    nativeQueueProducerPtr = 0;
    LOG.info("closing queue producer done.");
  }

  private native long writeMessageNative(long nativeQueueProducerPtr, long nativeIDPtr, long address, int size);

  private native void stopProducerNative(long nativeQueueProducerPtr);

  private native void closeProducerNative(long nativeQueueProducerPtr);

}
