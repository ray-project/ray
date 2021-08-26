package io.ray.streaming.runtime.transfer.channel;

import io.ray.streaming.runtime.config.StreamingWorkerConfig;
import io.ray.streaming.runtime.generated.Streaming;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelUtils.class);

  public static byte[] toNativeConf(StreamingWorkerConfig workerConfig) {
    Streaming.StreamingConfig.Builder builder = Streaming.StreamingConfig.newBuilder();

    // job name
    String jobName = workerConfig.commonConfig.jobName();
    if (!StringUtils.isEmpty(jobName)) {
      builder.setJobName(workerConfig.commonConfig.jobName());
    }

    // worker name
    String workerName = workerConfig.workerInternalConfig.workerName();
    if (!StringUtils.isEmpty(workerName)) {
      builder.setWorkerName(workerName);
    }

    // operator name
    String operatorName = workerConfig.workerInternalConfig.workerOperatorName();
    if (!StringUtils.isEmpty(operatorName)) {
      builder.setOpName(operatorName);
    }

    // ring buffer capacity
    int ringBufferCapacity = workerConfig.transferConfig.ringBufferCapacity();
    if (ringBufferCapacity != -1) {
      builder.setRingBufferCapacity(ringBufferCapacity);
    }

    // empty message interval
    int emptyMsgInterval = workerConfig.transferConfig.emptyMsgInterval();
    if (emptyMsgInterval != -1) {
      builder.setEmptyMessageInterval(emptyMsgInterval);
    }

    // flow control type
    int flowControlType = workerConfig.transferConfig.flowControlType();
    if (flowControlType != -1) {
      builder.setFlowControlType(Streaming.FlowControlType.forNumber(flowControlType));
    }

    // writer consumed step
    int writerConsumedStep = workerConfig.transferConfig.writerConsumedStep();
    if (writerConsumedStep != -1) {
      builder.setWriterConsumedStep(writerConsumedStep);
    }

    // reader consumed step
    int readerConsumedStep = workerConfig.transferConfig.readerConsumedStep();
    if (readerConsumedStep != -1) {
      builder.setReaderConsumedStep(readerConsumedStep);
    }

    Streaming.StreamingConfig streamingConf = builder.build();
    LOGGER.info("Streaming native conf {}", streamingConf.toString());
    return streamingConf.toByteArray();
  }
}
