package org.ray.streaming.queue.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.Queue;

import org.ray.streaming.queue.QueueConsumer;
import org.ray.streaming.queue.QueueID;
import org.ray.streaming.queue.QueueItem;
import org.ray.streaming.queue.QueueUtils;
import org.ray.streaming.util.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueConsumerImpl implements QueueConsumer {
  private final static Logger LOG = LoggerFactory.getLogger(QueueConsumerImpl.class);

  public enum MessageBundleType {
    EMPTY(1),
    BARRIER(2),
    BUNDLE(3);

    private int code;

    MessageBundleType(int code) {
      this.code = code;
    }
  }

  private long nativeQueueConsumerPtr;
  private Queue<QueueItem> buf;

  public QueueConsumerImpl(long nativeQueueConsumerPtr) {
    buf = new LinkedList<>();
    this.nativeQueueConsumerPtr = nativeQueueConsumerPtr;
  }

  class QueueBundleMeta {
    // kMessageBundleHeaderSize + kUniqueIDSize:
    // magicNum(4b) + bundleTs(8b) + lastMessageId(8b) + messageListSize(4b)
    // + bundleType(4b) + rawBundleSize(4b) + qid(20b)
    static final int LENGTH = 4 + 8 + 8 + 4 + 4 + 4 + 20;
    private int magicNum;
    private long bundleTs;
    private long lastMessageId;
    private int messageListSize;
    private MessageBundleType bundleType;
    private String qid;
    private int rawBundleSize;

    public QueueBundleMeta(ByteBuffer buffer) {
      // StreamingMessageBundleMeta Deserialization
      // magicNum
      magicNum = buffer.getInt();
      // messageBundleTs
      bundleTs = buffer.getLong();
      // lastOffsetSeqId
      lastMessageId = buffer.getLong();
      messageListSize = buffer.getInt();
      int bTypeInt = buffer.getInt();
      if (MessageBundleType.BUNDLE.code == bTypeInt) {
        bundleType = MessageBundleType.BUNDLE;
      } else if (MessageBundleType.BARRIER.code == bTypeInt) {
        bundleType = MessageBundleType.BARRIER;
      } else {
        bundleType = MessageBundleType.EMPTY;
      }
      // rawBundleSize
      rawBundleSize = buffer.getInt();
      qid = getQidString(buffer);
    }

    public int getMagicNum() {
      return magicNum;
    }

    public long getBundleTs() {
      return bundleTs;
    }

    public long getLastMessageId() {
      return lastMessageId;
    }

    public int getMessageListSize() {
      return messageListSize;
    }

    public MessageBundleType getBundleType() {
      return bundleType;
    }

    public String getQid() {
      return qid;
    }

    public int getRawBundleSize() {
      return rawBundleSize;
    }
  }

  // params set by getBundleNative: bundle data address + size
  private final ByteBuffer getBundleParams = ByteBuffer.allocateDirect(24);
  private final ByteBuffer bundleData = Platform.wrapDirectBuffer(0, 0);
  private final ByteBuffer bundleMeta = ByteBuffer.allocateDirect(QueueBundleMeta.LENGTH);

  {
    getBundleParams.order(ByteOrder.nativeOrder());
    bundleData.order(ByteOrder.nativeOrder());
    bundleMeta.order(ByteOrder.nativeOrder());
  }

  @Override
  public QueueItem pull(long timeoutMillis) {
    if (buf.isEmpty()) {
      getBundle(timeoutMillis);
      // if bundle not empty. empty message still has data size + seqId + msgId
      if (bundleData.position() < bundleData.limit()) {
        QueueBundleMeta queueBundleMeta = new QueueBundleMeta(bundleMeta);
        // barrier
        if (queueBundleMeta.getBundleType() == MessageBundleType.BARRIER) {
          throw new UnsupportedOperationException("Unsupported bundle type " + queueBundleMeta.getBundleType());
        } else if (queueBundleMeta.getBundleType() == MessageBundleType.BUNDLE) {
          String qid = queueBundleMeta.getQid();
          long timestamp = queueBundleMeta.getBundleTs();
          for (int i = 0; i < queueBundleMeta.getMessageListSize(); ++i) {
            buf.offer(getQueueMessage(bundleData, qid, timestamp));
          }
        } else if (queueBundleMeta.getBundleType() == MessageBundleType.EMPTY) {
          buf.offer(new QueueMessageImpl(queueBundleMeta.getQid(), null, queueBundleMeta.getBundleTs()));
        }
      }
    }
    if (buf.isEmpty()) {
      return null;
    }
    return buf.poll();
  }

  private QueueItem getQueueMessage(ByteBuffer bundleData, String qid, long timestamp) {
    int dataSize = bundleData.getInt();
    // seqId
    bundleData.getLong();
    // msgType
    bundleData.getInt();
    // make `data.capacity() == data.remaining()`, because some code used `capacity()` rather than `remaining()`
    int position = bundleData.position();
    int limit = bundleData.limit();
    bundleData.limit(position + dataSize);
    ByteBuffer data = bundleData.slice();
    bundleData.limit(limit);
    bundleData.position(position + dataSize);
    return new QueueMessageImpl(qid, data, timestamp);
  }

  private String getQidString(ByteBuffer buffer) {
    byte[] bytes = new byte[QueueID.ID_LENGTH];
    buffer.get(bytes);
    return QueueUtils.qidBytesToString(bytes);
  }

  private void getBundle(long timeoutMillis) {
    getBundleNative(nativeQueueConsumerPtr, timeoutMillis,
        Platform.getAddress(getBundleParams), Platform.getAddress(bundleMeta));
    bundleMeta.rewind();
    long bundleAddress = getBundleParams.getLong(0);
    int bundleSize = getBundleParams.getInt(8);
    // This has better performance than NewDirectBuffer or set address/capacity in jni.
    Platform.wrapDirectBuffer(bundleData, bundleAddress, bundleSize);
  }

  /**
   * Get bundle
   *
   * @param params      params buffer. Set bundle data address and size, bundle meta address and size into this buffer.
   * @param metaAddress bundle meta buffer address
   */
  private native void getBundleNative(long nativeQueueConsumerPtr, long timeoutMillis, long params, long metaAddress);

  @Override
  public void stop() {
    stopConsumerNative(nativeQueueConsumerPtr);
  }

  @Override
  public void close() {
    if (nativeQueueConsumerPtr == 0) {
      return;
    }
    LOG.info("closing queue consumer.");
    closeConsumerNative(nativeQueueConsumerPtr);
    nativeQueueConsumerPtr = 0;
    LOG.info("closing queue consumer done.");
  }

  private native void stopConsumerNative(long nativeQueueConsumerPtr);

  private native void closeConsumerNative(long nativeQueueConsumerPtr);

}
