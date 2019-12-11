package org.ray.streaming.runtime.queue.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ray.api.Ray;
import org.ray.api.id.ActorId;
import org.ray.api.runtime.RayRuntime;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.RayNativeRuntime;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.streaming.runtime.queue.QueueConsumer;
import org.ray.streaming.runtime.queue.QueueLink;
import org.ray.streaming.runtime.queue.QueueProducer;
import org.ray.streaming.runtime.transfer.ChannelUtils;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.util.ConfigKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingQueueLinkImpl implements QueueLink {

  static {
  }

  private static final Logger LOG = LoggerFactory.getLogger(StreamingQueueLinkImpl.class);
  private final Map<String, String> configuration = new HashMap<>();
  private Map<String, Object> inputCheckpoints = new HashMap<>();
  private Map<String, Object> outputCheckpoints = new HashMap<>();
  private QueueConsumerImpl consumerInstance = null;
  private QueueProducerImpl producerInstance = null;
  private long nativeMessageHandler = 0;
  private long nativeCoreWorker = 0;
  private JavaFunctionDescriptor streamingTransferFunction;
  private JavaFunctionDescriptor streamingTransferSyncFunction;
  private final Set<String> abnormalInputQueues = new HashSet<>();
  private final Set<String> abnormalOutputQueues = new HashSet<>();
  private Set<String> lastAbnormalQueues = new HashSet<>();
  private RayRuntime runtime = null;

  public StreamingQueueLinkImpl() {
    // Use JobWorker defaultly.
    streamingTransferFunction = new JavaFunctionDescriptor(JobWorker.class.getName(),
        "onStreamingTransfer", "([B)V");
    streamingTransferSyncFunction = new JavaFunctionDescriptor(JobWorker.class.getName(),
        "onStreamingTransferSync", "([B)[B");
  }

  @Override
  public void setRayRuntime(RayRuntime runtime) {
    this.runtime = runtime;
    try {
      configuration.put(QueueConfigKeys.PLASMA_STORE_PATH, Ray.getRuntimeContext().getObjectStoreSocketName());
      configuration.put(QueueConfigKeys.RAYLET_SOCKET_NAME, Ray.getRuntimeContext().getRayletSocketName());
      configuration.put(QueueConfigKeys.TASK_JOB_ID, Ray.getRuntimeContext().getCurrentJobId().toString());
      configuration.put(QueueConfigKeys.STREAMING_LOG_LEVEL, "-1");
    } catch (Exception e) {
      LOG.error("get params from runtime failed!", e);
    }
    createMessageHandler();
  }

  @Override
  public void setConfiguration(Map<String, String> conf) {
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      configuration.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public Map<String, String> getConfiguration() {
    return configuration;
  }

  @Override
  public QueueConsumer registerQueueConsumer(Collection<String> inputQueues, Map<String, ActorId> inputActorIdsMap) {
    if (this.consumerInstance != null) {
      return consumerInstance;
    }

    boolean isRecreate = false;
    if (configuration.containsKey(QueueConfigKeys.IS_RECREATE)) {
      isRecreate = Boolean.parseBoolean(configuration.get(QueueConfigKeys.IS_RECREATE));
    }
    long[] plasmaQueueSeqIds = new long[inputQueues.size()];
    long[] streamingMsgIds = new long[inputQueues.size()];
    // Using ArrayList to ensure both qid and actorhandle are in same order.
    Collection<String> inputQueueIds = new ArrayList<>();
    Collection<ActorId> inputActorIds = new ArrayList<>();

    int i = 0;
    for (String queue : inputQueues) {
      OffsetInfo offsetInfo = new OffsetInfo(0, 0);
      if (inputCheckpoints.containsKey(queue)) {
        offsetInfo = (OffsetInfo) inputCheckpoints.get(queue);
      }
      if (lastAbnormalQueues.contains(queue)) {
        offsetInfo.setSeqId(0);
      }
      plasmaQueueSeqIds[i] = offsetInfo.getSeqId();
      streamingMsgIds[i] = offsetInfo.getStreamingMsgId();
      inputQueueIds.add(queue);
      inputActorIds.add(inputActorIdsMap.get(queue));
      inputCheckpoints.put(queue, offsetInfo);
      i++;
    }

    LOG.info("register consumer, isRecreate:{}, queues:{}, seqIds: {}, conf={}, inputActorIds: {}",
        isRecreate, inputQueueIds, plasmaQueueSeqIds, configuration, inputActorIds);
    try {
      this.consumerInstance = new QueueConsumerImpl(newConsumer(
          nativeCoreWorker, ChannelUtils.actorIdListToByteArray(inputActorIds),
          streamingTransferFunction, streamingTransferSyncFunction,
          ChannelUtils.stringQueueIdListToByteArray(inputQueueIds),
          plasmaQueueSeqIds, streamingMsgIds,
          Long.parseLong(configuration.getOrDefault(QueueConfigKeys.TIMER_INTERVAL_MS, "-1")),
          isRecreate,
          FbsConfigConverter.map2bytes(configuration)
      ));
      LOG.info("Create QueueConsumerImpl success.");
    } catch (QueueInitException e) {
      LOG.warn("native consumer failed, abnormalQueues={}.", e.getAbnormalQueuesString());
      abnormalInputQueues.addAll(e.getAbnormalQueuesString());
    }
    return this.consumerInstance;
  }

  @Override
  public QueueProducer registerQueueProducer(Collection<String> outputQueues, Map<String, ActorId> outputActorIdsMap) {
    if (this.producerInstance != null) {
      return producerInstance;
    }

    long[] creatorTypes = new long[outputQueues.size()];
    // Using ArrayList to ensure both qid and actorhandle are in same order.
    Collection<String> outputQueueIds = new ArrayList<>();
    Collection<ActorId> outputActorIds = new ArrayList<>();

    int i = 0;
    for (String queue : outputQueues) {
      // RECONSTRUCT if has cp
      if (this.outputCheckpoints.containsKey(queue)) {
        creatorTypes[i] = 1;
      }
      // abnormal queues use RECREATE_AND_CLEAR
      if (lastAbnormalQueues.contains(queue)) {
        creatorTypes[i] = 2;
      }
      outputQueueIds.add(queue);
      outputActorIds.add(outputActorIdsMap.get(queue));
      i++;
    }

    List<Long> msgIds = new ArrayList<>();

    for (String qid : outputQueueIds) {
      long msgId = 0;
      if (outputCheckpoints.containsKey(qid)) {
        msgId = (long) outputCheckpoints.get(qid);
      }
      msgIds.add(msgId);
      outputCheckpoints.put(qid, msgId);
    }

    // convert to ordered list
    byte[][] qidCopyList = ChannelUtils.stringQueueIdListToByteArray(outputQueueIds);

    LOG.info("register producer, createType: {}, queues:{}, msgIds: {}, conf={}, outputActorIds:{}",
        creatorTypes, outputQueueIds, msgIds, configuration, outputActorIds);
    try {
      this.producerInstance = new QueueProducerImpl(newProducer(
          nativeCoreWorker, ChannelUtils.actorIdListToByteArray(outputActorIds),
          streamingTransferFunction, streamingTransferSyncFunction,
          qidCopyList, ChannelUtils.longToPrimitives(msgIds),
          Long.parseLong(configuration.get(ConfigKey.QUEUE_SIZE)),
          creatorTypes,
          FbsConfigConverter.map2bytes(configuration)
      ), qidCopyList);
      LOG.info("Create QueueProducerImpl success.");
    } catch (QueueInitException e) {
      LOG.warn("native producer failed, abnormalQueues={}.", e.getAbnormalQueuesString());
      abnormalOutputQueues.addAll(e.getAbnormalQueuesString());
    }
    return this.producerInstance;
  }

  @Override
  public void onQueueTransfer(byte[] buffer) {
    onQueueTransfer(nativeMessageHandler, buffer);
  }

  @Override
  public byte[] onQueueTransferSync(byte[] buffer) {
    return onQueueTransferSync(nativeMessageHandler, buffer);
  }

  public void setStreamingTransferFunction(JavaFunctionDescriptor streamingTransferFunction) {
    this.streamingTransferFunction = streamingTransferFunction;
  }

  public void setStreamingTransferSyncFunction(JavaFunctionDescriptor streamingTransferSyncFunction) {
    this.streamingTransferSyncFunction = streamingTransferSyncFunction;
  }

  // TODO: do not use reflection
  private long getNativeCoreWorker() {
    long pointer = 0;
    try {
      java.lang.reflect.Field pointerField = RayNativeRuntime.class.getDeclaredField("nativeCoreWorkerPointer");
      pointerField.setAccessible(true);
      pointer = (long) pointerField.get(((RayMultiWorkerNativeRuntime) runtime).getCurrentRuntime());
      LOG.info("getNativeCoreWorker: {}", pointer);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return pointer;
  }

  private long createMessageHandler() {
    if (0 == nativeCoreWorker) {
      nativeCoreWorker = getNativeCoreWorker();
    }

    if (0 == nativeMessageHandler) {
      nativeMessageHandler = newMessageHandler(nativeCoreWorker);
    }

    return nativeMessageHandler;
  }

  private native long newConsumer(
      long coreWorker,
      byte[][] inputActorIds,
      FunctionDescriptor asyncFunction,
      FunctionDescriptor syncFunction,
      byte[][] inputQueueIds,
      long[] plasmaQueueSeqIds,
      long[] streamingMsgIds,
      long timerInterval,
      boolean isRecreate,
      byte[] fbsConfigBytes
  ) throws QueueInitException;

  private native long newProducer(
      long coreWorker,
      byte[][] outputActorIds,
      FunctionDescriptor asyncFunction,
      FunctionDescriptor syncFunction,
      byte[][] outputQueueIds,
      long[] seqIds,
      long queueSize,
      long[] creatorTypes,
      byte[] fbsConfigBytes
  ) throws QueueInitException;

  private native long newMessageHandler(long core_worker);

  private native void onQueueTransfer(long handler, byte[] buffer);

  private native byte[] onQueueTransferSync(long handler, byte[] buffer);
}
