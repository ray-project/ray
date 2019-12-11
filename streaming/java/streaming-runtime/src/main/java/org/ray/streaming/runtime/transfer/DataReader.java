package org.ray.streaming.runtime.transfer;

import org.ray.api.id.ActorId;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.streaming.runtime.util.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class DataReader {
  private final static Logger LOG = LoggerFactory.getLogger(DataReader.class);

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
  private Queue<DataMessage> buf;
  public DataReader(List<String> inputChannels,
                    List<ActorId> fromActors,
                    Map<String, String> conf) {

  }

  public DataReader(long nativeQueueConsumerPtr) {
    buf = new LinkedList<>();
    this.nativeQueueConsumerPtr = nativeQueueConsumerPtr;
  }

  class QueueBundleMeta {
    // kMessageBundleHeaderSize + kUniqueIDSize:
    // magicNum(4b) + bundleTs(8b) + lastMessageId(8b) + messageListSize(4b)
    // + bundleType(4b) + rawBundleSize(4b) + channelID(20b)
    static final int LENGTH = 4 + 8 + 8 + 4 + 4 + 4 + 20;
    private int magicNum;
    private long bundleTs;
    private long lastMessageId;
    private int messageListSize;
    private MessageBundleType bundleType;
    private String channelID;
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
      channelID = getQidString(buffer);
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
      return channelID;
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

  /**
   * pull message from input queues, if timeout, return null.
   *
   * @param timeoutMillis timeout
   * @return message or null
   */
  public DataMessage pull(long timeoutMillis) {
    if (buf.isEmpty()) {
      getBundle(timeoutMillis);
      // if bundle not empty. empty message still has data size + seqId + msgId
      if (bundleData.position() < bundleData.limit()) {
        QueueBundleMeta queueBundleMeta = new QueueBundleMeta(bundleMeta);
        // barrier
        if (queueBundleMeta.getBundleType() == MessageBundleType.BARRIER) {
          throw new UnsupportedOperationException("Unsupported bundle type " + queueBundleMeta.getBundleType());
        } else if (queueBundleMeta.getBundleType() == MessageBundleType.BUNDLE) {
          String channelID = queueBundleMeta.getQid();
          long timestamp = queueBundleMeta.getBundleTs();
          for (int i = 0; i < queueBundleMeta.getMessageListSize(); ++i) {
            buf.offer(getQueueMessage(bundleData, channelID, timestamp));
          }
        } else if (queueBundleMeta.getBundleType() == MessageBundleType.EMPTY) {
          buf.offer(new DataMessage(null, queueBundleMeta.getBundleTs(),  queueBundleMeta.getQid()));
        }
      }
    }
    if (buf.isEmpty()) {
      return null;
    }
    return buf.poll();
  }

  private DataMessage getQueueMessage(ByteBuffer bundleData, String channelID, long timestamp) {
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
    return new DataMessage(data, timestamp, channelID);
  }

  private String getQidString(ByteBuffer buffer) {
    byte[] bytes = new byte[ChannelID.ID_LENGTH];
    buffer.get(bytes);
    return ChannelUtils.qidBytesToString(bytes);
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
   * stop consumer to avoid blocking
   */
  public void stop() {
    stopConsumerNative(nativeQueueConsumerPtr);
  }

  /**
   * close queue consumer to release resource
   */
  public void close() {
    if (nativeQueueConsumerPtr == 0) {
      return;
    }
    LOG.info("closing queue consumer.");
    closeConsumerNative(nativeQueueConsumerPtr);
    nativeQueueConsumerPtr = 0;
    LOG.info("closing queue consumer done.");
  }

  private native long createDataReaderNative(
      long coreWorker,
      byte[][] inputActorIds,
      FunctionDescriptor asyncFunction,
      FunctionDescriptor syncFunction,
      byte[][] inputQueueIds,
      long[] plasmaQueueSeqIds,
      long[] streamingMsgIds,
      long timerInterval,
      boolean isRecreate,
      byte[] fbsConfigBytes);

  private native void onTransfer(long handler, byte[] buffer);

  private native byte[] onTransferSync(long handler, byte[] buffer);

  private native void getBundleNative(long nativeQueueConsumerPtr, long timeoutMillis, long params, long metaAddress);

  private native void stopConsumerNative(long nativeQueueConsumerPtr);

  private native void closeConsumerNative(long nativeQueueConsumerPtr);

}
