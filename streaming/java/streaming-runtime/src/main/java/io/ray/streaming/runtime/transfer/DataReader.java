package io.ray.streaming.runtime.transfer;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.streaming.runtime.config.StreamingWorkerConfig;
import io.ray.streaming.runtime.config.types.TransferChannelType;
import io.ray.streaming.runtime.transfer.channel.ChannelId;
import io.ray.streaming.runtime.transfer.channel.ChannelRecoverInfo;
import io.ray.streaming.runtime.transfer.channel.ChannelRecoverInfo.ChannelCreationStatus;
import io.ray.streaming.runtime.transfer.channel.ChannelUtils;
import io.ray.streaming.runtime.transfer.channel.OffsetInfo;
import io.ray.streaming.runtime.transfer.message.BarrierMessage;
import io.ray.streaming.runtime.transfer.message.ChannelMessage;
import io.ray.streaming.runtime.transfer.message.DataMessage;
import io.ray.streaming.runtime.util.Platform;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataReader is wrapper of streaming c++ DataReader, which read data from channels of upstream
 * workers
 */
public class DataReader {

  private static final Logger LOG = LoggerFactory.getLogger(DataReader.class);

  private long nativeReaderPtr;
  // params set by getBundleNative: bundle data address + size
  private final ByteBuffer getBundleParams = ByteBuffer.allocateDirect(24);
  // We use direct buffer to reduce gc overhead and memory copy.
  private final ByteBuffer bundleData = Platform.wrapDirectBuffer(0, 0);
  private final ByteBuffer bundleMeta = ByteBuffer.allocateDirect(BundleMeta.LENGTH);

  private final Map<String, ChannelCreationStatus> queueCreationStatusMap = new HashMap<>();
  private Queue<ChannelMessage> buf = new LinkedList<>();

  {
    getBundleParams.order(ByteOrder.nativeOrder());
    bundleData.order(ByteOrder.nativeOrder());
    bundleMeta.order(ByteOrder.nativeOrder());
  }

  /**
   * @param inputChannels input channels ids
   * @param fromActors upstream input actors
   * @param workerConfig configuration
   */
  public DataReader(
      List<String> inputChannels,
      List<BaseActorHandle> fromActors,
      Map<String, OffsetInfo> checkpoints,
      StreamingWorkerConfig workerConfig) {
    Preconditions.checkArgument(inputChannels.size() > 0);
    Preconditions.checkArgument(inputChannels.size() == fromActors.size());
    ChannelCreationParametersBuilder initialParameters =
        new ChannelCreationParametersBuilder().buildInputQueueParameters(inputChannels, fromActors);
    byte[][] inputChannelsBytes =
        inputChannels.stream().map(ChannelId::idStrToBytes).toArray(byte[][]::new);

    // get sequence ID and message ID from OffsetInfo
    long[] msgIds = new long[inputChannels.size()];
    for (int i = 0; i < inputChannels.size(); i++) {
      String channelId = inputChannels.get(i);
      if (!checkpoints.containsKey(channelId)) {
        msgIds[i] = 0;
        continue;
      }
      msgIds[i] = checkpoints.get(inputChannels.get(i)).getStreamingMsgId();
    }
    long timerInterval = workerConfig.transferConfig.readerTimerIntervalMs();
    TransferChannelType channelType = workerConfig.transferConfig.channelType();
    boolean isMock = false;
    if (TransferChannelType.MEMORY_CHANNEL == channelType) {
      isMock = true;
    }

    // create native reader
    List<Integer> creationStatus = new ArrayList<>();
    this.nativeReaderPtr =
        createDataReaderNative(
            initialParameters,
            inputChannelsBytes,
            msgIds,
            timerInterval,
            creationStatus,
            ChannelUtils.toNativeConf(workerConfig),
            isMock);
    for (int i = 0; i < inputChannels.size(); ++i) {
      queueCreationStatusMap.put(
          inputChannels.get(i), ChannelCreationStatus.fromInt(creationStatus.get(i)));
    }
    LOG.info(
        "Create DataReader succeed for worker: {}, creation status={}.",
        workerConfig.workerInternalConfig.workerName(),
        queueCreationStatusMap);
  }

  private static native long createDataReaderNative(
      ChannelCreationParametersBuilder initialParameters,
      byte[][] inputChannels,
      long[] msgIds,
      long timerInterval,
      List<Integer> creationStatus,
      byte[] configBytes,
      boolean isMock);

  /**
   * Read message from input channels, if timeout, return null.
   *
   * @param timeoutMillis timeout
   * @return message or null
   */
  public ChannelMessage read(long timeoutMillis) {
    if (buf.isEmpty()) {
      getBundle(timeoutMillis);
      // if bundle not empty. empty message still has data size + seqId + msgId
      if (bundleData.position() < bundleData.limit()) {
        BundleMeta bundleMeta = new BundleMeta(this.bundleMeta);
        String channelID = bundleMeta.getChannelID();
        long timestamp = bundleMeta.getBundleTs();
        // barrier
        if (bundleMeta.getBundleType() == DataBundleType.BARRIER) {
          buf.offer(getBarrier(bundleData, channelID, timestamp));
        } else if (bundleMeta.getBundleType() == DataBundleType.BUNDLE) {
          for (int i = 0; i < bundleMeta.getMessageListSize(); i++) {
            buf.offer(getDataMessage(bundleData, channelID, timestamp));
          }
        }
      }
    }
    if (buf.isEmpty()) {
      return null;
    }
    return buf.poll();
  }

  public ChannelRecoverInfo getQueueRecoverInfo() {
    return new ChannelRecoverInfo(queueCreationStatusMap);
  }

  private String getQueueIdString(ByteBuffer buffer) {
    byte[] bytes = new byte[ChannelId.ID_LENGTH];
    buffer.get(bytes);
    return ChannelId.idBytesToStr(bytes);
  }

  private BarrierMessage getBarrier(ByteBuffer bundleData, String channelID, long timestamp) {
    ByteBuffer offsetsInfoBytes = ByteBuffer.wrap(getOffsetsInfoNative(nativeReaderPtr));
    offsetsInfoBytes.order(ByteOrder.nativeOrder());
    BarrierOffsetInfo offsetInfo = new BarrierOffsetInfo(offsetsInfoBytes);
    DataMessage message = getDataMessage(bundleData, channelID, timestamp);
    BarrierItem barrierItem = new BarrierItem(message, offsetInfo);
    return new BarrierMessage(
        message.getMsgId(),
        message.getTimestamp(),
        message.getChannelId(),
        barrierItem.getData(),
        barrierItem.getGlobalBarrierId(),
        barrierItem.getBarrierOffsetInfo().getQueueOffsetInfo());
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
    getBundleNative(
        nativeReaderPtr,
        timeoutMillis,
        Platform.getAddress(getBundleParams),
        Platform.getAddress(bundleMeta));
    bundleMeta.rewind();
    long bundleAddress = getBundleParams.getLong(0);
    int bundleSize = getBundleParams.getInt(8);
    // This has better performance than NewDirectBuffer or set address/capacity in jni.
    Platform.wrapDirectBuffer(bundleData, bundleAddress, bundleSize);
  }

  /** Stop reader */
  public void stop() {
    stopReaderNative(nativeReaderPtr);
  }

  /** Close reader to release resource */
  public void close() {
    if (nativeReaderPtr == 0) {
      return;
    }
    LOG.info("Closing DataReader.");
    closeReaderNative(nativeReaderPtr);
    nativeReaderPtr = 0;
    LOG.info("Finish closing DataReader.");
  }

  private native void getBundleNative(
      long nativeReaderPtr, long timeoutMillis, long params, long metaAddress);

  private native byte[] getOffsetsInfoNative(long nativeQueueConsumerPtr);

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

  public enum BarrierType {
    GLOBAL_BARRIER(0);
    private int code;

    BarrierType(int code) {
      this.code = code;
    }
  }

  class BundleMeta {

    // kMessageBundleHeaderSize + kUniqueIDSize:
    // magicNum(4b) + bundleTs(8b) + lastMessageId(8b) + messageListSize(4b)
    // + bundleType(4b) + rawBundleSize(4b) + channelID
    static final int LENGTH = 4 + 8 + 8 + 4 + 4 + 4 + ChannelId.ID_LENGTH;
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
      channelID = getQueueIdString(buffer);
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

  class BarrierOffsetInfo {

    private int queueSize;
    private Map<String, OffsetInfo> queueOffsetInfo;

    public BarrierOffsetInfo(ByteBuffer buffer) {
      // deserialization offset
      queueSize = buffer.getInt();
      queueOffsetInfo = new HashMap<>(queueSize);
      for (int i = 0; i < queueSize; ++i) {
        String qid = getQueueIdString(buffer);
        long streamingMsgId = buffer.getLong();
        queueOffsetInfo.put(qid, new OffsetInfo(streamingMsgId));
      }
    }

    public int getQueueSize() {
      return queueSize;
    }

    public Map<String, OffsetInfo> getQueueOffsetInfo() {
      return queueOffsetInfo;
    }
  }

  class BarrierItem {

    BarrierOffsetInfo barrierOffsetInfo;
    private long msgId;
    private BarrierType barrierType;
    private long globalBarrierId;
    private ByteBuffer data;

    public BarrierItem(DataMessage message, BarrierOffsetInfo barrierOffsetInfo) {
      this.barrierOffsetInfo = barrierOffsetInfo;
      msgId = message.getMsgId();
      ByteBuffer buffer = message.body();
      // c++ use native order, so use native order here.
      buffer.order(ByteOrder.nativeOrder());
      int barrierTypeInt = buffer.getInt();
      globalBarrierId = buffer.getLong();
      // dataSize includes: barrier type(32 bit), globalBarrierId, data
      data = buffer.slice();
      data.order(ByteOrder.nativeOrder());
      buffer.position(buffer.limit());
      barrierType = BarrierType.GLOBAL_BARRIER;
    }

    public long getBarrierMsgId() {
      return msgId;
    }

    public BarrierType getBarrierType() {
      return barrierType;
    }

    public long getGlobalBarrierId() {
      return globalBarrierId;
    }

    public ByteBuffer getData() {
      return data;
    }

    public BarrierOffsetInfo getBarrierOffsetInfo() {
      return barrierOffsetInfo;
    }
  }
}
