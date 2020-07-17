package io.ray.streaming.runtime.transfer;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.streaming.runtime.config.StreamingWorkerConfig;
import io.ray.streaming.runtime.config.types.TransferChannelType;
import io.ray.streaming.runtime.util.Platform;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataReader is wrapper of streaming c++ DataReader, which read data
 * from channels of upstream workers
 */
public class DataReader {
  private static final Logger LOG = LoggerFactory.getLogger(DataReader.class);

  private long nativeReaderPtr;
  private Queue<DataMessage> buf = new LinkedList<>();

  /**
   * @param inputChannels input channels ids
   * @param fromActors    upstream input actors
   * @param workerConfig  configuration
   */
  public DataReader(List<String> inputChannels,
                    List<BaseActorHandle> fromActors,
                    StreamingWorkerConfig workerConfig) {
    Preconditions.checkArgument(inputChannels.size() > 0);
    Preconditions.checkArgument(inputChannels.size() == fromActors.size());
    ChannelCreationParametersBuilder initialParameters =
        new ChannelCreationParametersBuilder().buildInputQueueParameters(inputChannels, fromActors);
    byte[][] inputChannelsBytes = inputChannels.stream()
        .map(ChannelId::idStrToBytes).toArray(byte[][]::new);
    long[] seqIds = new long[inputChannels.size()];
    long[] msgIds = new long[inputChannels.size()];
    for (int i = 0; i < inputChannels.size(); i++) {
      seqIds[i] = 0;
      msgIds[i] = 0;
    }
    long timerInterval = workerConfig.transferConfig.readerTimerIntervalMs();
    TransferChannelType channelType = workerConfig.transferConfig.channelType();
    boolean isMock = false;
    if (TransferChannelType.MEMORY_CHANNEL == channelType) {
      isMock = true;
    }
    boolean isRecreate = workerConfig.transferConfig.readerIsRecreate();

    this.nativeReaderPtr = createDataReaderNative(
        initialParameters,
        inputChannelsBytes,
        seqIds,
        msgIds,
        timerInterval,
        isRecreate,
        ChannelUtils.toNativeConf(workerConfig),
        isMock
    );
    LOG.info("Create DataReader succeed for worker: {}.",
        workerConfig.workerInternalConfig.workerName());
  }

  // params set by getBundleNative: bundle data address + size
  private final ByteBuffer getBundleParams = ByteBuffer.allocateDirect(24);
  // We use direct buffer to reduce gc overhead and memory copy.
  private final ByteBuffer bundleData = Platform.wrapDirectBuffer(0, 0);
  private final ByteBuffer bundleMeta = ByteBuffer.allocateDirect(BundleMeta.LENGTH);

  {
    getBundleParams.order(ByteOrder.nativeOrder());
    bundleData.order(ByteOrder.nativeOrder());
    bundleMeta.order(ByteOrder.nativeOrder());
  }

  /**
   * Read message from input channels, if timeout, return null.
   *
   * @param timeoutMillis timeout
   * @return message or null
   */
  public DataMessage read(long timeoutMillis) {
    if (buf.isEmpty()) {
      getBundle(timeoutMillis);
      // if bundle not empty. empty message still has data size + seqId + msgId
      if (bundleData.position() < bundleData.limit()) {
        BundleMeta bundleMeta = new BundleMeta(this.bundleMeta);
        // barrier
        if (bundleMeta.getBundleType() == DataBundleType.BARRIER) {
          throw new UnsupportedOperationException(
              "Unsupported bundle type " + bundleMeta.getBundleType());
        } else if (bundleMeta.getBundleType() == DataBundleType.BUNDLE) {
          String channelID = bundleMeta.getChannelID();
          long timestamp = bundleMeta.getBundleTs();
          for (int i = 0; i < bundleMeta.getMessageListSize(); i++) {
            buf.offer(getDataMessage(bundleData, channelID, timestamp));
          }
        } else if (bundleMeta.getBundleType() == DataBundleType.EMPTY) {
          long messageId = bundleMeta.getLastMessageId();
          buf.offer(new DataMessage(null, bundleMeta.getBundleTs(),
              messageId, bundleMeta.getChannelID()));
        }
      }
    }
    if (buf.isEmpty()) {
      return null;
    }
    return buf.poll();
  }

  private DataMessage getDataMessage(ByteBuffer bundleData, String channelID, long timestamp) {
    int dataSize = bundleData.getInt();
    // msgId
    long msgId = bundleData.getLong();
    // msgType
    bundleData.getInt();
    // make `data.capacity() == data.remaining()`, because some code used `capacity()`
    // rather than `remaining()`
    int position = bundleData.position();
    int limit = bundleData.limit();
    bundleData.limit(position + dataSize);
    ByteBuffer data = bundleData.slice();
    bundleData.limit(limit);
    bundleData.position(position + dataSize);
    return new DataMessage(data, timestamp, msgId, channelID);
  }

  private void getBundle(long timeoutMillis) {
    getBundleNative(nativeReaderPtr, timeoutMillis,
        Platform.getAddress(getBundleParams), Platform.getAddress(bundleMeta));
    bundleMeta.rewind();
    long bundleAddress = getBundleParams.getLong(0);
    int bundleSize = getBundleParams.getInt(8);
    // This has better performance than NewDirectBuffer or set address/capacity in jni.
    Platform.wrapDirectBuffer(bundleData, bundleAddress, bundleSize);
  }

  /**
   * Stop reader
   */
  public void stop() {
    stopReaderNative(nativeReaderPtr);
  }

  /**
   * Close reader to release resource
   */
  public void close() {
    if (nativeReaderPtr == 0) {
      return;
    }
    LOG.info("Closing DataReader.");
    closeReaderNative(nativeReaderPtr);
    nativeReaderPtr = 0;
    LOG.info("Finish closing DataReader.");
  }

  private static native long createDataReaderNative(
      ChannelCreationParametersBuilder initialParameters,
      byte[][] inputChannels,
      long[] seqIds,
      long[] msgIds,
      long timerInterval,
      boolean isRecreate,
      byte[] configBytes,
      boolean isMock);

  private native void getBundleNative(long nativeReaderPtr,
                                      long timeoutMillis,
                                      long params,
                                      long metaAddress);

  private native void stopReaderNative(long nativeReaderPtr);

  private native void closeReaderNative(long nativeReaderPtr);

  enum DataBundleType {
    EMPTY(1),
    BARRIER(2),
    BUNDLE(3);

    int code;

    DataBundleType(int code) {
      this.code = code;
    }
  }

  static class BundleMeta {
    // kMessageBundleHeaderSize + kUniqueIDSize:
    // magicNum(4b) + bundleTs(8b) + lastMessageId(8b) + messageListSize(4b)
    // + bundleType(4b) + rawBundleSize(4b) + channelID(20b)
    static final int LENGTH = 4 + 8 + 8 + 4 + 4 + 4 + 20;
    private int magicNum;
    private long bundleTs;
    private long lastMessageId;
    private int messageListSize;
    private DataBundleType bundleType;
    private String channelID;
    private int rawBundleSize;

    BundleMeta(ByteBuffer buffer) {
      // StreamingMessageBundleMeta Deserialization
      // magicNum
      magicNum = buffer.getInt();
      // messageBundleTs
      bundleTs = buffer.getLong();
      // lastOffsetSeqId
      lastMessageId = buffer.getLong();
      messageListSize = buffer.getInt();
      int typeInt = buffer.getInt();
      if (DataBundleType.BUNDLE.code == typeInt) {
        bundleType = DataBundleType.BUNDLE;
      } else if (DataBundleType.BARRIER.code == typeInt) {
        bundleType = DataBundleType.BARRIER;
      } else {
        bundleType = DataBundleType.EMPTY;
      }
      // rawBundleSize
      rawBundleSize = buffer.getInt();
      channelID = getQidString(buffer);
    }

    private String getQidString(ByteBuffer buffer) {
      byte[] bytes = new byte[ChannelId.ID_LENGTH];
      buffer.get(bytes);
      return ChannelId.idBytesToStr(bytes);
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

    public DataBundleType getBundleType() {
      return bundleType;
    }

    public String getChannelID() {
      return channelID;
    }

    public int getRawBundleSize() {
      return rawBundleSize;
    }
  }

}
